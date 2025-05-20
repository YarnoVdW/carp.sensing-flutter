/// This is the runtime which performs the actual data collection on a device (e.g.,
/// desktop computer or smartphone). This subsystem contains reusable components
/// which understand the runtime configuration derived from a study protocol by
/// the ‘deployment’ subsystem. Integrations with sensors are loaded through a
/// 'device data collector' plug-in system to decouple sensing — not part of core —
/// from sensing logic.
///
/// [ClientManager] is the main entry point into this subsystem.
/// Concrete devices extend on it, e.g., the SmartphoneClient manages data
/// collection on a smartphone and is implemented in
/// [CARP Mobile Sensing](https://pub.dev/packages/carp_mobile_sensing).
///
/// Contains the core client classes like [ClientManager], [DeviceDataCollectorFactory],
/// [DeviceDataCollector], and [StudyRuntime].
///
/// See the [`carp.clients`](https://github.com/cph-cachet/carp.core-kotlin/blob/develop/docs/carp-clients.md)
/// definition in Kotlin.
library;

import 'dart:async';
import 'package:meta/meta.dart';
import 'package:carp_core/common/carp_core_common.dart';
import 'package:carp_core/deployment/carp_core_deployment.dart';

part 'client_manager.dart';
part 'study_runtime.dart';
part 'study.dart';
part 'device_data_collector.dart';
