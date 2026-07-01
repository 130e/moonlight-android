# Moonlight Android

## Frame Metrics and Raw Bitstream Capture

This branch adds optional stream capture for offline analysis:
- Raw encoded video units (`video.<ext>`)
- Per-sample index records (`sample_index.jsonl`)
- Per-frame stats records (`frame_stats.jsonl`, optional)
- Rolling network status records (`network_stats.jsonl`, optional)
- Session metadata (`session.json`)

### Build and install

- Ensure Android NDK is configured in `local.properties` (replace `{HOME}`):
```
ndk.dir={HOME}/Android/Sdk/ndk/27.0.12077973
sdk.dir={HOME}/Android/Sdk
```
- Fetch submodules:
```shell
git submodule update --init --recursive
```
- Apply the `moonlight-common-c` patch (adds the RTP video packet-loss counters used by
  `network_stats.jsonl`). The submodule tracks upstream `moonlight-stream/moonlight-common-c`
  at `8af4562`. The modification is kept as a patch rather than a separate submodule fork:
```shell
git apply --directory=app/src/main/jni/moonlight-core/moonlight-common-c \
    patches/moonlight-common-c-netstats.patch
```
- Build (JDK 17+):
```shell
./gradlew :app:assembleNonRootDebug # or :app:assembleRootDebug
```
- Install:
```shell
adb install app/build/outputs/apk/nonRoot/debug/app-nonRoot-debug.apk
```

### Run capture

- Open Moonlight on the device.
- In Settings, enable `Capture received video`.
- Set `Capture size limit per session`.
- Optional: keep `Save per-frame statistics` enabled for `frame_stats.jsonl`.
- Start a stream.

Capture output location:
`/storage/emulated/0/Android/data/com.limelight.debug/files/captures/`

Each stream creates a session directory named like:
`yyyyMMdd-HHmmss-<codec>-<width>x<height>@<fps>`

### Output traces

#### `session.json` (JSON object)
One metadata file per session.

Fields:
- `created_wall_time`
- `codec` (`h264`, `hevc`, `av1`, or `unknown`)
- `width`, `height`, `fps`
- `video_format_mask`
- `capture_mode` (`raw_only`)
- `bitstream_file`
- `sample_index_file`
- `session_dir`

#### `video.<ext>` (raw bitstream)
Raw decode units appended in receive order.

Extension mapping:
- `.h264` for H.264
- `.h265` for HEVC
- `.av1` for AV1
- `.bin` fallback

#### `sample_index.jsonl` (JSON Lines)
One JSON object per line. Every record includes:
- `event`
- `seq` (monotonic index sequence)
- `uptime_ms` (Android uptime timestamp)

Event payloads:
- `session_start`: `codec`, `width`, `height`, `fps`, `cap_bytes`, `bitstream_file`, `capture_mode`
- `sample`: `frame_number`, `frame_type`, `pts_us`, `receive_time_ms`, `enqueue_time_ms`, `file_offset`, `sample_size`, `bitstream_file`
- `csd`: `decode_unit_type`, `frame_number`, `pts_us`, `file_offset`, `sample_size`, `bitstream_file`
- `session_end`: `reason`, `estimated_video_bytes`, `cap_reached`

#### `frame_stats.jsonl` (JSON Lines, optional)
Written only when `Save per-frame statistics` is enabled. Every record includes:
- `event`
- `uptime_ms`

Event payloads:
- `session_start`: `codec`, `width`, `height`, `fps`, `cap_bytes`, `video_file`, `sample_index_file`
- `capture_mode`: `video_format`, `reason`
- `frame_received`: `frame_number`, `frame_type`, `decode_unit_length`, `host_processing_latency_0_1ms`, `receive_time_ms`, `enqueue_time_ms`, `pts_us`
- `frame_decoded`: `frame_number`, `frame_type`, `pts_us`, `decoder_latency_ms`, `queue_delay_ms` (or fallback `-1` for frame metadata when unmatched)
- `network_frame_drop`: `previous_frame_number`, `next_frame_number`, `dropped_frame_count`, `first_dropped_frame_number`, `last_dropped_frame_number`
- `jitter_frame_drop`: `frame_number`, `frame_type`, `pts_us`, `reason` (or fallback `-1` for frame metadata when unmatched)
- `capture_stopped`: `reason`
- `session_end`: `reason`, `estimated_video_bytes`, `cap_reached`

#### `network_stats.jsonl` (JSON Lines, optional)
Written only when `Save per-frame statistics` is enabled. Captures the rolling network
condition (RTT and downlink video packet loss) sampled roughly once per second, rather than
per frame. Every record includes:
- `event`
- `uptime_ms`

Event payloads:
- `session_start`: `codec`, `width`, `height`, `fps`
- `network_stats`:
  - `enet_rtt_ms`, `enet_rtt_variance_ms` — ENet control-channel RTT estimate at sample time (`-1` if the peer is not connected)
  - `expected_data_packets`, `received_data_packets`, `lost_data_packets` — cumulative video data-packet counts since connection start (`lost = expected - received`)
  - `expected_parity_packets`, `received_parity_packets`, `lost_parity_packets` — cumulative FEC parity-packet counts
  - `window_expected_data_packets`, `window_received_data_packets`, `window_lost_data_packets` — deltas over the sample window
  - `window_expected_parity_packets`, `window_received_parity_packets` — parity deltas over the window
  - `window_data_loss_ratio` — `window_lost_data_packets / window_expected_data_packets` for the window (`0` on the first sample)
- `session_end`: `reason`

### Inferred stats

- `frame_received` and index records are emitted when decode units arrive at the decoder input path (`onDecodeUnit`).
- `frame_decoded` is emitted when the frame is released from `MediaCodec` output (`onFrameDecoded`), using PTS to match received metadata.
- `network_frame_drop` is emitted when frame-number gaps are detected at the decoder input path.
- `jitter_frame_drop` is emitted when decoded output is discarded by renderer pacing before display submission.
- `network_stats` is sampled at the ~1 s stats-window flip (not per frame): RTT comes from ENet (`MoonBridge.getEstimatedRttInfo`) and packet counts from the RTP layer (`MoonBridge.getVideoPacketStats`). The packet counters are cumulative and FEC-aware; "expected" is booked per FEC block as frames arrive, so frames never fully received still count toward loss.
- Raw bytes are written continuously until the session ends or the configured size cap is reached.
- Hitting the cap stops capture only (streaming continues), records `capture_stopped`/`session_end`, and shows a toast on device.

### Pull captures from device

```shell
adb pull /storage/emulated/0/Android/data/com.limelight.debug/files/captures/
```

**WIP**: Add tooling to reconstruct frames (for example YUV) using `sample_index.jsonl` + `video.<ext>`.

## Original Moonlight Android README

[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/232a8tadrrn8jv0k/branch/master?svg=true)](https://ci.appveyor.com/project/cgutman/moonlight-android/branch/master)
[![Translation Status](https://hosted.weblate.org/widgets/moonlight/-/moonlight-android/svg-badge.svg)](https://hosted.weblate.org/projects/moonlight/moonlight-android/)

[Moonlight for Android](https://moonlight-stream.org) is an open source client for NVIDIA GameStream and [Sunshine](https://github.com/LizardByte/Sunshine).

Moonlight for Android will allow you to stream your full collection of games from your Windows PC to your Android device,
whether in your own home or over the internet.

Moonlight also has a [PC client](https://github.com/moonlight-stream/moonlight-qt) and [iOS/tvOS client](https://github.com/moonlight-stream/moonlight-ios).

You can follow development on our [Discord server](https://moonlight-stream.org/discord) and help translate Moonlight into your language on [Weblate](https://hosted.weblate.org/projects/moonlight/moonlight-android/).

## Downloads
* [Google Play Store](https://play.google.com/store/apps/details?id=com.limelight)
* [Amazon App Store](https://www.amazon.com/gp/product/B00JK4MFN2)
* [F-Droid](https://f-droid.org/packages/com.limelight)
* [APK](https://github.com/moonlight-stream/moonlight-android/releases)

## Building
* Install Android Studio and the Android NDK
* Run ‘git submodule update --init --recursive’ from within moonlight-android/
* In moonlight-android/, create a file called ‘local.properties’. Add an ‘ndk.dir=’ property to the local.properties file and set it equal to your NDK directory.
* Build the APK using Android Studio or gradle

## Authors

* [Cameron Gutman](https://github.com/cgutman)  
* [Diego Waxemberg](https://github.com/dwaxemberg)  
* [Aaron Neyer](https://github.com/Aaronneyer)  
* [Andrew Hennessy](https://github.com/yetanothername)

Moonlight is the work of students at [Case Western](http://case.edu) and was
started as a project at [MHacks](http://mhacks.org).
