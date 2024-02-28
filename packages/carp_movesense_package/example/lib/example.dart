import 'package:carp_core/carp_core.dart';
import 'package:carp_mobile_sensing/carp_mobile_sensing.dart';
import 'package:carp_movesense_package/carp_movesense_package.dart';

/// This is a very simple example of how this sampling package is used as part
/// of defining a study protocol in CARP Mobile Sensing (CAMS).
///
/// NOTE, however, that the code below will not run on it own. A study protocol
/// needs to be deployed and executed in the CAMS framework.
///
/// See the documentation on how to use CAMS:
/// https://github.com/cph-cachet/carp.sensing-flutter/wiki
void main() async {
  // register this sampling package before using its measures
  SamplingPackageRegistry().register(MovesenseSamplingPackage());

  // Create a study protocol
  StudyProtocol protocol = StudyProtocol(
    ownerId: 'owner@dtu.dk',
    name: 'Movesense Sensing Example',
  );

  // define which devices are used for data collection - both phone and eSense
  var phone = Smartphone();
  var movesense = MovesenseDevice(
    serial: '220330000122',
    address: '0C:8C:DC:3F:B2:CD',
    name: 'Movesense Medical',
    deviceType: MovesenseDeviceType.MD,
  );

  protocol
    ..addPrimaryDevice(phone)
    ..addConnectedDevice(movesense, phone);

  // Add a background task that immediately starts collecting step counts,
  //ambient light, screen activity, and battery level from the phone.
  protocol.addTaskControl(
      ImmediateTrigger(),
      BackgroundTask()
        ..addMeasure(Measure(type: SensorSamplingPackage.STEP_COUNT))
        ..addMeasure(Measure(type: SensorSamplingPackage.AMBIENT_LIGHT))
        ..addMeasure(Measure(type: DeviceSamplingPackage.SCREEN_EVENT))
        ..addMeasure(Measure(type: DeviceSamplingPackage.BATTERY_STATE)),
      phone);

  // Add a background task that immediately starts collecting HR and ECG data
  // from the Polar device.
  protocol.addTaskControl(
      ImmediateTrigger(),
      BackgroundTask(measures: [
        Measure(type: MovesenseSamplingPackage.HR),
        Measure(type: MovesenseSamplingPackage.ECG),
      ]),
      movesense);
}
