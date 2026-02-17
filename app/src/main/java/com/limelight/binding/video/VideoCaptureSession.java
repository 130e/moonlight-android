package com.limelight.binding.video;

import android.content.Context;
import android.os.SystemClock;

import com.limelight.LimeLog;
import com.limelight.nvstream.jni.MoonBridge;
import com.limelight.preferences.PreferenceConfiguration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

class VideoCaptureSession {
    private static final class FrameMeta {
        int frameNumber;
        int frameType;
        long receiveTimeMs;
        long enqueueTimeMs;

        FrameMeta(int frameNumber, int frameType, long receiveTimeMs, long enqueueTimeMs) {
            this.frameNumber = frameNumber;
            this.frameType = frameType;
            this.receiveTimeMs = receiveTimeMs;
            this.enqueueTimeMs = enqueueTimeMs;
        }
    }

    private final boolean statsEnabled;
    private final long capBytes;
    private final int videoFormat;
    private final int width;
    private final int height;
    private final int fps;
    private final Runnable capReachedCallback;
    private final Map<Long, FrameMeta> framesByPtsUs = new HashMap<>();
    private final File sessionDir;

    private boolean closed;
    private boolean capReached;
    private long estimatedVideoBytes;

    private BufferedWriter statsWriter;
    private BufferedWriter indexWriter;
    private OutputStream rawVideoStream;
    private File videoFile;
    private File indexFile;
    private long fileWriteOffset;
    private long indexSeq;

    VideoCaptureSession(Context context, PreferenceConfiguration prefs, int videoFormat,
                        int width, int height, int fps, Runnable capReachedCallback) {
        this.videoFormat = videoFormat;
        this.width = width;
        this.height = height;
        this.fps = fps;
        this.capReachedCallback = capReachedCallback;
        this.statsEnabled = prefs.enableFrameStatsCapture;
        // Maximum capture size
        this.capBytes = Math.max(1, prefs.videoCaptureCapMb) * 1024L * 1024L;

        if (!prefs.enableVideoCapture) {
            sessionDir = null;
            closed = true;
            return;
        }

        File baseDir = context.getExternalFilesDir(null);
        if (baseDir == null) {
            baseDir = context.getFilesDir();
        }

        String codecLabel = codecLabel(videoFormat);
        String timestamp = new SimpleDateFormat("yyyyMMdd-HHmmss", Locale.US).format(new Date());
        sessionDir = new File(new File(baseDir, "captures"),
                timestamp + "-" + codecLabel + "-" + width + "x" + height + "@" + fps);

        if (!sessionDir.mkdirs()) {
            if (!sessionDir.exists()) {
                LimeLog.severe("Failed to create capture directory: " + sessionDir.getAbsolutePath());
                closed = true;
                return;
            }
        }

        videoFile = new File(sessionDir, "video" + rawExtension(videoFormat));
        indexFile = new File(sessionDir, "sample_index.jsonl");

        try {
            rawVideoStream = new FileOutputStream(videoFile, false);
        } catch (IOException e) {
            LimeLog.severe("Failed to open raw capture file: " + e);
            closed = true;
            return;
        }

        writeSessionMetadata(codecLabel);
        initializeStatsWriter(codecLabel);
        initializeIndexWriter(codecLabel);
        writeStatsEvent("capture_mode", "\"video_format\":\"raw\",\"reason\":\"raw_only_forced\"");
    }

    synchronized boolean isActive() {
        return !closed;
    }

    synchronized void onDecodeUnit(byte[] decodeUnitData, int decodeUnitLength, int decodeUnitType,
                                   int frameNumber, int frameType, char frameHostProcessingLatency,
                                   long receiveTimeMs, long enqueueTimeMs, long ptsUs) {
        if (closed) {
            return;
        }

        if (decodeUnitType == MoonBridge.BUFFER_TYPE_PICDATA) {
            framesByPtsUs.put(ptsUs, new FrameMeta(frameNumber, frameType, receiveTimeMs, enqueueTimeMs));
            if (statsEnabled) {
                writeStatsEvent("frame_received",
                        "\"frame_number\":" + frameNumber +
                        ",\"frame_type\":" + frameType +
                        ",\"decode_unit_length\":" + decodeUnitLength +
                        ",\"host_processing_latency_0_1ms\":" + (int) frameHostProcessingLatency +
                        ",\"receive_time_ms\":" + receiveTimeMs +
                        ",\"enqueue_time_ms\":" + enqueueTimeMs +
                        ",\"pts_us\":" + ptsUs);
            }
        }

        long offsetBeforeWrite = fileWriteOffset;
        if (!writeRaw(decodeUnitData, decodeUnitLength)) {
            return;
        }

        if (decodeUnitType == MoonBridge.BUFFER_TYPE_PICDATA) {
            writeSampleIndexRecord(frameNumber, frameType, ptsUs, receiveTimeMs, enqueueTimeMs,
                    offsetBeforeWrite, decodeUnitLength);
        } else if (decodeUnitType == MoonBridge.BUFFER_TYPE_SPS ||
                decodeUnitType == MoonBridge.BUFFER_TYPE_PPS ||
                decodeUnitType == MoonBridge.BUFFER_TYPE_VPS) {
            writeCsdIndexRecord(decodeUnitType, frameNumber, ptsUs, offsetBeforeWrite, decodeUnitLength);
        }
    }

    synchronized void onFrameDecoded(long presentationTimeUs, long decoderLatencyMs) {
        if (closed || !statsEnabled) {
            return;
        }

        FrameMeta meta = framesByPtsUs.remove(presentationTimeUs);
        if (meta != null) {
            writeStatsEvent("frame_decoded",
                    "\"frame_number\":" + meta.frameNumber +
                    ",\"frame_type\":" + meta.frameType +
                    ",\"pts_us\":" + presentationTimeUs +
                    ",\"decoder_latency_ms\":" + decoderLatencyMs +
                    ",\"queue_delay_ms\":" + Math.max(0, meta.enqueueTimeMs - meta.receiveTimeMs));
        } else {
            writeStatsEvent("frame_decoded",
                    "\"frame_number\":-1" +
                    ",\"frame_type\":-1" +
                    ",\"pts_us\":" + presentationTimeUs +
                    ",\"decoder_latency_ms\":" + decoderLatencyMs);
        }
    }

    synchronized void onSessionEnd(String reason) {
        if (closed) {
            return;
        }

        writeIndexEvent("session_end",
                "\"reason\":\"" + escapeJson(reason) + "\"" +
                ",\"estimated_video_bytes\":" + estimatedVideoBytes +
                ",\"cap_reached\":" + capReached);
        writeStatsEvent("session_end",
                "\"reason\":\"" + escapeJson(reason) + "\"" +
                ",\"estimated_video_bytes\":" + estimatedVideoBytes +
                ",\"cap_reached\":" + capReached);
        closeInternal();
    }

    private boolean writeRaw(byte[] data, int length) {
        if (rawVideoStream == null || closed) {
            return false;
        }
        if (!checkCap(length)) {
            return false;
        }

        try {
            rawVideoStream.write(data, 0, length);
            estimatedVideoBytes += length;
            fileWriteOffset += length;
            return true;
        } catch (IOException e) {
            LimeLog.severe("Failed to write raw video sample: " + e);
            onSessionEnd("write_error");
            return false;
        }
    }

    private boolean checkCap(long writeBytes) {
        if (capReached || closed) {
            return false;
        }
        if (estimatedVideoBytes + writeBytes <= capBytes) {
            return true;
        }

        capReached = true;
        writeStatsEvent("capture_stopped", "\"reason\":\"cap_reached\"");
        onSessionEnd("cap_reached");

        if (capReachedCallback != null) {
            capReachedCallback.run();
        }
        return false;
    }

    private void closeInternal() {
        if (closed) {
            return;
        }
        closed = true;

        if (rawVideoStream != null) {
            try {
                rawVideoStream.flush();
                rawVideoStream.close();
            } catch (IOException ignored) {
            }
            rawVideoStream = null;
        }

        if (statsWriter != null) {
            try {
                statsWriter.flush();
                statsWriter.close();
            } catch (IOException ignored) {
            }
            statsWriter = null;
        }

        if (indexWriter != null) {
            try {
                indexWriter.flush();
                indexWriter.close();
            } catch (IOException ignored) {
            }
            indexWriter = null;
        }
    }

    private void initializeStatsWriter(String codecLabel) {
        if (!statsEnabled || sessionDir == null) {
            return;
        }

        File statsFile = new File(sessionDir, "frame_stats.jsonl");
        try {
            statsWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(statsFile, false), StandardCharsets.UTF_8));
            writeStatsEvent("session_start",
                    "\"codec\":\"" + codecLabel + "\"" +
                    ",\"width\":" + width +
                    ",\"height\":" + height +
                    ",\"fps\":" + fps +
                    ",\"cap_bytes\":" + capBytes +
                    ",\"video_file\":\"" + escapeJson(videoFile.getName()) + "\"" +
                    ",\"sample_index_file\":\"" + escapeJson(indexFile.getName()) + "\"");
        } catch (IOException e) {
            LimeLog.severe("Failed to open stats file: " + e);
            statsWriter = null;
        }
    }

    private void initializeIndexWriter(String codecLabel) {
        if (sessionDir == null) {
            return;
        }

        try {
            indexWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(indexFile, false), StandardCharsets.UTF_8));
            writeIndexEvent("session_start",
                    "\"codec\":\"" + codecLabel + "\"" +
                    ",\"width\":" + width +
                    ",\"height\":" + height +
                    ",\"fps\":" + fps +
                    ",\"cap_bytes\":" + capBytes +
                    ",\"bitstream_file\":\"" + escapeJson(videoFile.getName()) + "\"" +
                    ",\"capture_mode\":\"raw_only\"");
        } catch (IOException e) {
            LimeLog.severe("Failed to open sample index file: " + e);
            indexWriter = null;
        }
    }

    private void writeSessionMetadata(String codecLabel) {
        File metadataFile = new File(sessionDir, "session.json");
        String json = "{"
                + "\"created_wall_time\":\"" + escapeJson(new Date().toString()) + "\","
                + "\"codec\":\"" + codecLabel + "\","
                + "\"width\":" + width + ","
                + "\"height\":" + height + ","
                + "\"fps\":" + fps + ","
                + "\"video_format_mask\":" + videoFormat + ","
                + "\"capture_mode\":\"raw_only\","
                + "\"bitstream_file\":\"" + escapeJson(videoFile.getName()) + "\","
                + "\"sample_index_file\":\"" + escapeJson(indexFile.getName()) + "\","
                + "\"session_dir\":\"" + escapeJson(sessionDir.getAbsolutePath()) + "\""
                + "}";

        try (OutputStream out = new FileOutputStream(metadataFile, false)) {
            out.write(json.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            LimeLog.warning("Failed to write session metadata: " + e);
        }
    }

    private void writeStatsEvent(String type, String fields) {
        if (statsWriter == null || closed) {
            return;
        }

        String line = "{"
                + "\"event\":\"" + escapeJson(type) + "\","
                + "\"uptime_ms\":" + SystemClock.uptimeMillis()
                + (fields.isEmpty() ? "" : "," + fields)
                + "}\n";

        try {
            statsWriter.write(line);
        } catch (IOException e) {
            LimeLog.warning("Failed to write stats event: " + e);
        }
    }

    private void writeIndexEvent(String type, String fields) {
        if (indexWriter == null || closed) {
            return;
        }

        String line = "{"
                + "\"event\":\"" + escapeJson(type) + "\","
                + "\"seq\":" + indexSeq++ + ","
                + "\"uptime_ms\":" + SystemClock.uptimeMillis()
                + (fields.isEmpty() ? "" : "," + fields)
                + "}\n";

        try {
            indexWriter.write(line);
        } catch (IOException e) {
            LimeLog.warning("Failed to write index event: " + e);
        }
    }

    private void writeSampleIndexRecord(int frameNumber, int frameType, long ptsUs,
                                        long receiveTimeMs, long enqueueTimeMs,
                                        long fileOffset, int sampleSize) {
        writeIndexEvent("sample",
                "\"frame_number\":" + frameNumber +
                ",\"frame_type\":" + frameType +
                ",\"pts_us\":" + ptsUs +
                ",\"receive_time_ms\":" + receiveTimeMs +
                ",\"enqueue_time_ms\":" + enqueueTimeMs +
                ",\"file_offset\":" + fileOffset +
                ",\"sample_size\":" + sampleSize +
                ",\"bitstream_file\":\"" + escapeJson(videoFile.getName()) + "\"");
    }

    private void writeCsdIndexRecord(int decodeUnitType, int frameNumber, long ptsUs,
                                     long fileOffset, int sampleSize) {
        writeIndexEvent("csd",
                "\"decode_unit_type\":" + decodeUnitType +
                ",\"frame_number\":" + frameNumber +
                ",\"pts_us\":" + ptsUs +
                ",\"file_offset\":" + fileOffset +
                ",\"sample_size\":" + sampleSize +
                ",\"bitstream_file\":\"" + escapeJson(videoFile.getName()) + "\"");
    }

    private static String codecLabel(int videoFormat) {
        if ((videoFormat & MoonBridge.VIDEO_FORMAT_MASK_H264) != 0) {
            return "h264";
        }
        if ((videoFormat & MoonBridge.VIDEO_FORMAT_MASK_H265) != 0) {
            return "hevc";
        }
        if ((videoFormat & MoonBridge.VIDEO_FORMAT_MASK_AV1) != 0) {
            return "av1";
        }
        return "unknown";
    }

    private static String rawExtension(int videoFormat) {
        if ((videoFormat & MoonBridge.VIDEO_FORMAT_MASK_H264) != 0) {
            return ".h264";
        }
        if ((videoFormat & MoonBridge.VIDEO_FORMAT_MASK_H265) != 0) {
            return ".h265";
        }
        if ((videoFormat & MoonBridge.VIDEO_FORMAT_MASK_AV1) != 0) {
            return ".av1";
        }
        return ".bin";
    }

    private static String escapeJson(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
