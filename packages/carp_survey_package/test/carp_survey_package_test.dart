import 'dart:convert';
import 'dart:io';
import 'package:test/test.dart';

import 'package:carp_serializable/carp_serializable.dart';
import 'package:carp_core/carp_core.dart';
import 'package:carp_mobile_sensing/carp_mobile_sensing.dart';
import 'package:carp_survey_package/survey.dart';

void main() {
  late StudyProtocol protocol;
  Smartphone phone;

  setUp(() {
    // Initialization of serialization
    CarpMobileSensing();

    // register the survey sampling package
    SamplingPackageRegistry().register(SurveySamplingPackage());

    // Create a new study protocol.
    protocol = StudyProtocol(
      ownerId: 'alex@uni.dk',
      name: 'Context package test',
    );

    // Define which devices are used for data collection.
    phone = Smartphone();

    protocol.addPrimaryDevice(phone);

    // adding all available measures to one one trigger and one task
    protocol.addTaskControl(
      ImmediateTrigger(),
      BackgroundTask()
        ..measures = SamplingPackageRegistry()
            .dataTypes
            .map((type) => Measure(type: type.type))
            .toList(),
      phone,
    );

    // add a WHO-5 survey as an app task
    // plus collect device and ambient light information when survey is done
    protocol.addTaskControl(
        DelayedTrigger(delay: const Duration(seconds: 30)),
        RPAppTask(
            type: SurveyUserTask.SURVEY_TYPE,
            name: 'WHO-5 Survey',
            rpTask: who5Task,
            measures: [
              Measure(type: DeviceSamplingPackage.DEVICE_INFORMATION),
              Measure(type: SensorSamplingPackage.AMBIENT_LIGHT),
            ]),
        phone);
  });

  test('CAMSStudyProtocol -> JSON', () async {
    print(protocol);
    print(toJsonString(protocol));
    expect(protocol.ownerId, 'alex@uni.dk');
  });

  test('StudyProtocol -> JSON -> StudyProtocol :: deep assert', () async {
    print('#1 : $protocol');
    final studyJson = toJsonString(protocol);

    StudyProtocol protocolFromJson =
        StudyProtocol.fromJson(json.decode(studyJson) as Map<String, dynamic>);
    expect(toJsonString(protocolFromJson), equals(studyJson));
    print('#2 : $protocolFromJson');
  });

  test('JSON File -> StudyProtocol', () async {
    // Read the study protocol from json file
    String plainJson = File('test/json/study_protocol.json').readAsStringSync();

    StudyProtocol protocol =
        StudyProtocol.fromJson(json.decode(plainJson) as Map<String, dynamic>);

    expect(protocol.ownerId, 'alex@uni.dk');
    expect(protocol.primaryDevice.roleName, Smartphone.DEFAULT_ROLENAME);
    expect((protocol.tasks.last as RPAppTask).type, SurveyUserTask.SURVEY_TYPE);
    print(toJsonString(protocol));
  });
}
