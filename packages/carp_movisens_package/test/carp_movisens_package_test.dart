import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:carp_movisens_package/carp_movisens_package.dart';
import 'package:test/test.dart';

import 'package:carp_serializable/carp_serializable.dart';
import 'package:carp_core/carp_core.dart';
import 'package:carp_mobile_sensing/carp_mobile_sensing.dart';
import 'package:openmhealth_schemas/openmhealth_schemas.dart' as omh;

void main() {
  late StudyProtocol protocol;
  Smartphone phone;

  setUp(() {
    // Initialization of serialization
    CarpMobileSensing();

    // register the context sampling package
    SamplingPackageRegistry().register(MovisensSamplingPackage());

    // create a new study protocol
    protocol = StudyProtocol(
      ownerId: 'alex@uni.dk',
      name: 'Context package test',
    );

    // define the Movisens device used for data collection
    phone = Smartphone();
    MovisensDevice movisens = MovisensDevice(
      sensorLocation: SensorLocation.Chest,
      sex: Sex.Male,
      deviceName: 'Sensor 02655',
      height: 175,
      weight: 75,
      age: 25,
    );

    protocol
      ..addPrimaryDevice(phone)
      ..addConnectedDevice(movisens, phone);

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

    // add a background task that immediately starts collecting Movisens events
    protocol.addTaskControl(
        ImmediateTrigger(),
        BackgroundTask(measures: [
          Measure(type: MovisensSamplingPackage.ACTIVITY),
          Measure(type: MovisensSamplingPackage.HR),
          Measure(type: MovisensSamplingPackage.EDA),
          Measure(type: MovisensSamplingPackage.SKIN_TEMPERATURE),
          Measure(type: MovisensSamplingPackage.TAP_MARKER),
        ]),
        movisens);
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
    expect(protocol.connectedDevices?.first.roleName,
        MovisensDevice.DEFAULT_ROLE_NAME);

    print(toJsonString(protocol));
  });

  test('Movisens HR -> OMH HeartRate', () {
    MovisensHR hr = MovisensHR(
      deviceId: 'unit_test_device_name',
      hr: 78,
      type: 'hrMean',
    );

    final dp_1 = Measurement.fromData(hr);
    print(toJsonString(dp_1));
    expect(dp_1.dataType.namespace, MovisensSamplingPackage.HR);

    final omhHR = DataTransformerSchemaRegistry()
        .lookup(NameSpace.OMH)!
        .transform(hr) as OMHHeartRateDataPoint;
    ;
    final dp_2 = Measurement.fromData(omhHR);
    print(toJsonString(dp_2));

    expect(dp_2.dataType.namespace, NameSpace.OMH);
    expect(omhHR.datapoint.body, isA<omh.HeartRate>());
    var hr_2 = omhHR.datapoint.body as omh.HeartRate;
    expect(hr_2.heartRate.value, hr.hr);
  });

  test('Movisens Step Count -> OMH StepCount', () {
    MovisensStepCount steps = MovisensStepCount(
      deviceId: 'unit_test_device_name',
      steps: 56,
      type: 'steps',
      timestamp: DateTime.now().toUtc(),
    );

    final m_1 = Measurement.fromData(steps);
    print(toJsonString(m_1));
    expect(m_1.dataType.namespace, MovisensSamplingPackage.ACTIVITY);

    final omhSteps = DataTransformerSchemaRegistry()
        .lookup(NameSpace.OMH)!
        .transform(steps) as OMHStepCountDataPoint;
    final m_2 = Measurement.fromData(omhSteps);
    print(toJsonString(m_2));

    expect(m_2.dataType.namespace, NameSpace.OMH);
    expect(omhSteps.datapoint.body, isA<omh.StepCount>());
    var steps_2 = omhSteps.datapoint.body as omh.StepCount;
    expect(steps_2.stepCount, steps.steps);
  });

  test('Movisens HR -> FHIR Heart Rate Observation', () {
    MovisensHR hr = MovisensHR(
      deviceId: 'unit_test_device_name',
      hr: 118,
      type: 'hrMean',
    );

    final m_1 = Measurement.fromData(hr);
    print(toJsonString(m_1));
    expect(m_1.dataType.namespace, MovisensSamplingPackage.HR);

    final fhirHR = DataTransformerSchemaRegistry()
        .lookup(NameSpace.FHIR)!
        .transform(hr) as FHIRHeartRateObservation;
    final m_2 = Measurement.fromData(fhirHR);
    print(toJsonString(m_2));

    expect(m_2.dataType.namespace, NameSpace.FHIR);
    expect(fhirHR.fhirJson["resourceType"], "Observation");
    expect(fhirHR.fhirJson["valueQuantity"]["value"], hr.hr);
  });

  test('Movisens HR -> OMH HR Data Point Example', () {
    MovisensHR data = MovisensHR(
      deviceId: 'unit_test_device_name',
      hr: 118,
      type: 'hrMean',
    );

    var transformedData =
        DataTransformerSchemaRegistry().lookup(NameSpace.OMH)!.transform(data);

    Stream<Data> dataStream = StreamController<Data>().stream;

    Stream<Data> transformedDataStream = dataStream.map((data) => data =
        DataTransformerSchemaRegistry().lookup(NameSpace.OMH)!.transform(data));

    Stream<Data> transformedPrivateDataStream = dataStream.map((data) => data =
        DataTransformerSchemaRegistry().lookup(NameSpace.OMH)!.transform(
            DataTransformerSchemaRegistry()
                .lookup("privacySchemaName")!
                .transform(data)));
  });
}
