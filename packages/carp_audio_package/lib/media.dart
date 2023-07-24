/// A library for collecting an audio recording from the phone's microphone.
/// Support the following measures:
///  * audio recording
///  * noise sampling
library media;

import 'dart:async';
import 'dart:io';

import 'package:flutter_sound/flutter_sound.dart';
import 'package:json_annotation/json_annotation.dart';
import 'package:noise_meter/noise_meter.dart';
import 'package:stats/stats.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:uuid/uuid.dart';

import 'package:carp_serializable/carp_serializable.dart';
import 'package:carp_core/carp_core.dart';
import 'package:carp_mobile_sensing/carp_mobile_sensing.dart';

part 'audio_probe.dart';
part 'video_probe.dart';
part 'noise_probe.dart';
part 'media_data.dart';
part 'media_package.dart';
part 'media.g.dart';
