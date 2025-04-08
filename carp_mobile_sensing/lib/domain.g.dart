// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'domain.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

SmartphoneApplicationData _$SmartphoneApplicationDataFromJson(
        Map<String, dynamic> json) =>
    SmartphoneApplicationData(
      studyDescription: json['studyDescription'] == null
          ? null
          : StudyDescription.fromJson(
              json['studyDescription'] as Map<String, dynamic>),
      dataEndPoint: json['dataEndPoint'] == null
          ? null
          : DataEndPoint.fromJson(json['dataEndPoint'] as Map<String, dynamic>),
      privacySchemaName: json['privacySchemaName'] as String?,
      applicationData: json['applicationData'] as Map<String, dynamic>?,
    );

Map<String, dynamic> _$SmartphoneApplicationDataToJson(
        SmartphoneApplicationData instance) =>
    <String, dynamic>{
      if (instance.studyDescription?.toJson() case final value?)
        'studyDescription': value,
      if (instance.dataEndPoint?.toJson() case final value?)
        'dataEndPoint': value,
      if (instance.privacySchemaName case final value?)
        'privacySchemaName': value,
      if (instance.applicationData case final value?) 'applicationData': value,
    };

SmartphoneStudyProtocol _$SmartphoneStudyProtocolFromJson(
        Map<String, dynamic> json) =>
    SmartphoneStudyProtocol(
      ownerId: json['ownerId'] as String?,
      name: json['name'] as String,
    )
      ..applicationData = json['applicationData'] as Map<String, dynamic>?
      ..id = json['id'] as String
      ..createdOn = DateTime.parse(json['createdOn'] as String)
      ..version = (json['version'] as num).toInt()
      ..description = json['description'] as String
      ..participantRoles = (json['participantRoles'] as List<dynamic>?)
          ?.map((e) => ParticipantRole.fromJson(e as Map<String, dynamic>))
          .toSet()
      ..primaryDevices = (json['primaryDevices'] as List<dynamic>)
          .map((e) => PrimaryDeviceConfiguration<DeviceRegistration>.fromJson(
              e as Map<String, dynamic>))
          .toSet()
      ..connectedDevices = (json['connectedDevices'] as List<dynamic>?)
          ?.map((e) => DeviceConfiguration<DeviceRegistration>.fromJson(
              e as Map<String, dynamic>))
          .toSet()
      ..connections = (json['connections'] as List<dynamic>?)
          ?.map((e) => DeviceConnection.fromJson(e as Map<String, dynamic>))
          .toList()
      ..assignedDevices =
          (json['assignedDevices'] as Map<String, dynamic>?)?.map(
        (k, e) =>
            MapEntry(k, (e as List<dynamic>).map((e) => e as String).toSet()),
      )
      ..tasks = (json['tasks'] as List<dynamic>)
          .map((e) => TaskConfiguration.fromJson(e as Map<String, dynamic>))
          .toSet()
      ..triggers = (json['triggers'] as Map<String, dynamic>).map(
        (k, e) => MapEntry(
            k, TriggerConfiguration.fromJson(e as Map<String, dynamic>)),
      )
      ..taskControls = (json['taskControls'] as List<dynamic>)
          .map((e) => TaskControl.fromJson(e as Map<String, dynamic>))
          .toSet()
      ..expectedParticipantData =
          (json['expectedParticipantData'] as List<dynamic>?)
              ?.map((e) =>
                  ExpectedParticipantData.fromJson(e as Map<String, dynamic>))
              .toSet();

Map<String, dynamic> _$SmartphoneStudyProtocolToJson(
        SmartphoneStudyProtocol instance) =>
    <String, dynamic>{
      if (instance.applicationData case final value?) 'applicationData': value,
      'id': instance.id,
      'createdOn': instance.createdOn.toIso8601String(),
      'version': instance.version,
      'description': instance.description,
      'ownerId': instance.ownerId,
      'name': instance.name,
      if (instance.participantRoles?.map((e) => e.toJson()).toList()
          case final value?)
        'participantRoles': value,
      'primaryDevices': instance.primaryDevices.map((e) => e.toJson()).toList(),
      if (instance.connectedDevices?.map((e) => e.toJson()).toList()
          case final value?)
        'connectedDevices': value,
      if (instance.connections?.map((e) => e.toJson()).toList()
          case final value?)
        'connections': value,
      if (instance.assignedDevices?.map((k, e) => MapEntry(k, e.toList()))
          case final value?)
        'assignedDevices': value,
      'tasks': instance.tasks.map((e) => e.toJson()).toList(),
      'triggers': instance.triggers.map((k, e) => MapEntry(k, e.toJson())),
      'taskControls': instance.taskControls.map((e) => e.toJson()).toList(),
      if (instance.expectedParticipantData?.map((e) => e.toJson()).toList()
          case final value?)
        'expectedParticipantData': value,
    };

StudyDescription _$StudyDescriptionFromJson(Map<String, dynamic> json) =>
    StudyDescription(
      title: json['title'] as String,
      description: json['description'] as String?,
      purpose: json['purpose'] as String?,
      studyDescriptionUrl: json['studyDescriptionUrl'] as String?,
      privacyPolicyUrl: json['privacyPolicyUrl'] as String?,
      responsible: json['responsible'] == null
          ? null
          : StudyResponsible.fromJson(
              json['responsible'] as Map<String, dynamic>),
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$StudyDescriptionToJson(StudyDescription instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      'title': instance.title,
      if (instance.description case final value?) 'description': value,
      if (instance.purpose case final value?) 'purpose': value,
      if (instance.studyDescriptionUrl case final value?)
        'studyDescriptionUrl': value,
      if (instance.privacyPolicyUrl case final value?)
        'privacyPolicyUrl': value,
      if (instance.responsible?.toJson() case final value?)
        'responsible': value,
    };

StudyResponsible _$StudyResponsibleFromJson(Map<String, dynamic> json) =>
    StudyResponsible(
      id: json['id'] as String,
      name: json['name'] as String,
      title: json['title'] as String?,
      email: json['email'] as String?,
      affiliation: json['affiliation'] as String?,
      address: json['address'] as String?,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$StudyResponsibleToJson(StudyResponsible instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      'id': instance.id,
      'name': instance.name,
      if (instance.title case final value?) 'title': value,
      if (instance.email case final value?) 'email': value,
      if (instance.address case final value?) 'address': value,
      if (instance.affiliation case final value?) 'affiliation': value,
    };

DataEndPoint _$DataEndPointFromJson(Map<String, dynamic> json) => DataEndPoint(
      type: json['type'] as String,
      dataFormat: json['dataFormat'] as String? ?? NameSpace.CARP,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$DataEndPointToJson(DataEndPoint instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      'type': instance.type,
      'dataFormat': instance.dataFormat,
    };

FileDataEndPoint _$FileDataEndPointFromJson(Map<String, dynamic> json) =>
    FileDataEndPoint(
      type: json['type'] as String? ?? DataEndPointTypes.FILE,
      dataFormat: json['dataFormat'] as String? ?? NameSpace.CARP,
      bufferSize: (json['bufferSize'] as num?)?.toInt() ?? 500 * 1000,
      zip: json['zip'] as bool? ?? true,
      encrypt: json['encrypt'] as bool? ?? false,
      publicKey: json['publicKey'] as String?,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$FileDataEndPointToJson(FileDataEndPoint instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      'type': instance.type,
      'dataFormat': instance.dataFormat,
      'bufferSize': instance.bufferSize,
      'zip': instance.zip,
      'encrypt': instance.encrypt,
      if (instance.publicKey case final value?) 'publicKey': value,
    };

SQLiteDataEndPoint _$SQLiteDataEndPointFromJson(Map<String, dynamic> json) =>
    SQLiteDataEndPoint(
      type: json['type'] as String? ?? DataEndPointTypes.SQLITE,
      dataFormat: json['dataFormat'] as String? ?? NameSpace.CARP,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$SQLiteDataEndPointToJson(SQLiteDataEndPoint instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      'type': instance.type,
      'dataFormat': instance.dataFormat,
    };

PersistentSamplingConfiguration _$PersistentSamplingConfigurationFromJson(
        Map<String, dynamic> json) =>
    PersistentSamplingConfiguration()
      ..$type = json['__type'] as String?
      ..lastTime = json['lastTime'] == null
          ? null
          : DateTime.parse(json['lastTime'] as String);

Map<String, dynamic> _$PersistentSamplingConfigurationToJson(
        PersistentSamplingConfiguration instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.lastTime?.toIso8601String() case final value?)
        'lastTime': value,
    };

HistoricSamplingConfiguration _$HistoricSamplingConfigurationFromJson(
        Map<String, dynamic> json) =>
    HistoricSamplingConfiguration(
      past: json['past'] == null
          ? null
          : Duration(microseconds: (json['past'] as num).toInt()),
      future: json['future'] == null
          ? null
          : Duration(microseconds: (json['future'] as num).toInt()),
    )
      ..$type = json['__type'] as String?
      ..lastTime = json['lastTime'] == null
          ? null
          : DateTime.parse(json['lastTime'] as String);

Map<String, dynamic> _$HistoricSamplingConfigurationToJson(
        HistoricSamplingConfiguration instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.lastTime?.toIso8601String() case final value?)
        'lastTime': value,
      'past': instance.past.inMicroseconds,
      'future': instance.future.inMicroseconds,
    };

IntervalSamplingConfiguration _$IntervalSamplingConfigurationFromJson(
        Map<String, dynamic> json) =>
    IntervalSamplingConfiguration(
      interval: Duration(microseconds: (json['interval'] as num).toInt()),
    )
      ..$type = json['__type'] as String?
      ..lastTime = json['lastTime'] == null
          ? null
          : DateTime.parse(json['lastTime'] as String);

Map<String, dynamic> _$IntervalSamplingConfigurationToJson(
        IntervalSamplingConfiguration instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.lastTime?.toIso8601String() case final value?)
        'lastTime': value,
      'interval': instance.interval.inMicroseconds,
    };

PeriodicSamplingConfiguration _$PeriodicSamplingConfigurationFromJson(
        Map<String, dynamic> json) =>
    PeriodicSamplingConfiguration(
      interval: Duration(microseconds: (json['interval'] as num).toInt()),
      duration: Duration(microseconds: (json['duration'] as num).toInt()),
    )
      ..$type = json['__type'] as String?
      ..lastTime = json['lastTime'] == null
          ? null
          : DateTime.parse(json['lastTime'] as String);

Map<String, dynamic> _$PeriodicSamplingConfigurationToJson(
        PeriodicSamplingConfiguration instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.lastTime?.toIso8601String() case final value?)
        'lastTime': value,
      'interval': instance.interval.inMicroseconds,
      'duration': instance.duration.inMicroseconds,
    };

OnlineService<TRegistration> _$OnlineServiceFromJson<
        TRegistration extends DeviceRegistration>(Map<String, dynamic> json) =>
    OnlineService<TRegistration>(
      roleName: json['roleName'] as String,
      isOptional: json['isOptional'] as bool? ?? true,
    )
      ..$type = json['__type'] as String?
      ..defaultSamplingConfiguration =
          (json['defaultSamplingConfiguration'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(
            k, SamplingConfiguration.fromJson(e as Map<String, dynamic>)),
      );

Map<String, dynamic>
    _$OnlineServiceToJson<TRegistration extends DeviceRegistration>(
            OnlineService<TRegistration> instance) =>
        <String, dynamic>{
          if (instance.$type case final value?) '__type': value,
          'roleName': instance.roleName,
          if (instance.isOptional case final value?) 'isOptional': value,
          if (instance.defaultSamplingConfiguration
                  ?.map((k, e) => MapEntry(k, e.toJson()))
              case final value?)
            'defaultSamplingConfiguration': value,
        };

SmartphoneDeployment _$SmartphoneDeploymentFromJson(
        Map<String, dynamic> json) =>
    SmartphoneDeployment(
      studyId: json['studyId'] as String?,
      studyDeploymentId: json['studyDeploymentId'] as String?,
      deviceConfiguration:
          PrimaryDeviceConfiguration<DeviceRegistration>.fromJson(
              json['deviceConfiguration'] as Map<String, dynamic>),
      registration: DeviceRegistration.fromJson(
          json['registration'] as Map<String, dynamic>),
      connectedDevices: (json['connectedDevices'] as List<dynamic>?)
              ?.map((e) => DeviceConfiguration<DeviceRegistration>.fromJson(
                  e as Map<String, dynamic>))
              .toSet() ??
          const {},
      connectedDeviceRegistrations: (json['connectedDeviceRegistrations']
                  as Map<String, dynamic>?)
              ?.map(
            (k, e) => MapEntry(
                k,
                e == null
                    ? null
                    : DeviceRegistration.fromJson(e as Map<String, dynamic>)),
          ) ??
          const {},
      tasks: (json['tasks'] as List<dynamic>?)
              ?.map(
                  (e) => TaskConfiguration.fromJson(e as Map<String, dynamic>))
              .toSet() ??
          const {},
      triggers: (json['triggers'] as Map<String, dynamic>?)?.map(
            (k, e) => MapEntry(
                k, TriggerConfiguration.fromJson(e as Map<String, dynamic>)),
          ) ??
          const {},
      taskControls: (json['taskControls'] as List<dynamic>?)
              ?.map((e) => TaskControl.fromJson(e as Map<String, dynamic>))
              .toSet() ??
          const {},
      expectedParticipantData: (json['expectedParticipantData']
                  as List<dynamic>?)
              ?.map((e) =>
                  ExpectedParticipantData.fromJson(e as Map<String, dynamic>))
              .toSet() ??
          const {},
      participantId: json['participantId'] as String?,
      participantRoleName: json['participantRoleName'] as String?,
    )
      ..applicationData = json['applicationData'] as Map<String, dynamic>?
      ..deployed = json['deployed'] == null
          ? null
          : DateTime.parse(json['deployed'] as String)
      ..status = $enumDecode(_$StudyStatusEnumMap, json['status']);

Map<String, dynamic> _$SmartphoneDeploymentToJson(
        SmartphoneDeployment instance) =>
    <String, dynamic>{
      if (instance.applicationData case final value?) 'applicationData': value,
      'deviceConfiguration': instance.deviceConfiguration.toJson(),
      'registration': instance.registration.toJson(),
      'connectedDevices':
          instance.connectedDevices.map((e) => e.toJson()).toList(),
      'connectedDeviceRegistrations': instance.connectedDeviceRegistrations
          .map((k, e) => MapEntry(k, e?.toJson())),
      'tasks': instance.tasks.map((e) => e.toJson()).toList(),
      'triggers': instance.triggers.map((k, e) => MapEntry(k, e.toJson())),
      'taskControls': instance.taskControls.map((e) => e.toJson()).toList(),
      'expectedParticipantData':
          instance.expectedParticipantData.map((e) => e.toJson()).toList(),
      if (instance.studyId case final value?) 'studyId': value,
      'studyDeploymentId': instance.studyDeploymentId,
      if (instance.participantId case final value?) 'participantId': value,
      if (instance.participantRoleName case final value?)
        'participantRoleName': value,
      if (instance.deployed?.toIso8601String() case final value?)
        'deployed': value,
      'status': _$StudyStatusEnumMap[instance.status]!,
    };

const _$StudyStatusEnumMap = {
  StudyStatus.DeploymentNotStarted: 'DeploymentNotStarted',
  StudyStatus.DeploymentStatusAvailable: 'DeploymentStatusAvailable',
  StudyStatus.Deploying: 'Deploying',
  StudyStatus.AwaitingOtherDeviceRegistrations:
      'AwaitingOtherDeviceRegistrations',
  StudyStatus.AwaitingDeviceDeployment: 'AwaitingDeviceDeployment',
  StudyStatus.DeviceDeploymentReceived: 'DeviceDeploymentReceived',
  StudyStatus.RegisteringDevices: 'RegisteringDevices',
  StudyStatus.Deployed: 'Deployed',
  StudyStatus.Running: 'Running',
  StudyStatus.Stopped: 'Stopped',
  StudyStatus.DeploymentNotAvailable: 'DeploymentNotAvailable',
};

AppTask _$AppTaskFromJson(Map<String, dynamic> json) => AppTask(
      name: json['name'] as String?,
      measures: (json['measures'] as List<dynamic>?)
          ?.map((e) => Measure.fromJson(e as Map<String, dynamic>))
          .toList(),
      type: json['type'] as String,
      title: json['title'] as String? ?? '',
      description: json['description'] as String? ?? '',
      instructions: json['instructions'] as String? ?? '',
      minutesToComplete: (json['minutesToComplete'] as num?)?.toInt(),
      expire: json['expire'] == null
          ? null
          : Duration(microseconds: (json['expire'] as num).toInt()),
      notification: json['notification'] as bool? ?? false,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$AppTaskToJson(AppTask instance) => <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      'name': instance.name,
      if (instance.measures?.map((e) => e.toJson()).toList() case final value?)
        'measures': value,
      'type': instance.type,
      'title': instance.title,
      'description': instance.description,
      'instructions': instance.instructions,
      if (instance.minutesToComplete case final value?)
        'minutesToComplete': value,
      if (instance.expire?.inMicroseconds case final value?) 'expire': value,
      'notification': instance.notification,
    };

FunctionTask _$FunctionTaskFromJson(Map<String, dynamic> json) => FunctionTask(
      name: json['name'] as String?,
      description: json['description'] as String?,
    )
      ..$type = json['__type'] as String?
      ..measures = (json['measures'] as List<dynamic>?)
          ?.map((e) => Measure.fromJson(e as Map<String, dynamic>))
          .toList();

Map<String, dynamic> _$FunctionTaskToJson(FunctionTask instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      'name': instance.name,
      if (instance.measures?.map((e) => e.toJson()).toList() case final value?)
        'measures': value,
      if (instance.description case final value?) 'description': value,
    };

NoOpTrigger _$NoOpTriggerFromJson(Map<String, dynamic> json) => NoOpTrigger()
  ..$type = json['__type'] as String?
  ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$NoOpTriggerToJson(NoOpTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
    };

ImmediateTrigger _$ImmediateTriggerFromJson(Map<String, dynamic> json) =>
    ImmediateTrigger()
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$ImmediateTriggerToJson(ImmediateTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
    };

OneTimeTrigger _$OneTimeTriggerFromJson(Map<String, dynamic> json) =>
    OneTimeTrigger()
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?
      ..triggerTimestamp = json['triggerTimestamp'] == null
          ? null
          : DateTime.parse(json['triggerTimestamp'] as String);

Map<String, dynamic> _$OneTimeTriggerToJson(OneTimeTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      if (instance.triggerTimestamp?.toIso8601String() case final value?)
        'triggerTimestamp': value,
    };

PassiveTrigger _$PassiveTriggerFromJson(Map<String, dynamic> json) =>
    PassiveTrigger()
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$PassiveTriggerToJson(PassiveTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
    };

DelayedTrigger _$DelayedTriggerFromJson(Map<String, dynamic> json) =>
    DelayedTrigger(
      delay: Duration(microseconds: (json['delay'] as num).toInt()),
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$DelayedTriggerToJson(DelayedTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'delay': instance.delay.inMicroseconds,
    };

PeriodicTrigger _$PeriodicTriggerFromJson(Map<String, dynamic> json) =>
    PeriodicTrigger(
      period: Duration(microseconds: (json['period'] as num).toInt()),
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$PeriodicTriggerToJson(PeriodicTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'period': instance.period.inMicroseconds,
    };

DateTimeTrigger _$DateTimeTriggerFromJson(Map<String, dynamic> json) =>
    DateTimeTrigger(
      schedule: DateTime.parse(json['schedule'] as String),
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$DateTimeTriggerToJson(DateTimeTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'schedule': instance.schedule.toIso8601String(),
    };

RecurrentScheduledTrigger _$RecurrentScheduledTriggerFromJson(
        Map<String, dynamic> json) =>
    RecurrentScheduledTrigger(
      type: $enumDecode(_$RecurrentTypeEnumMap, json['type']),
      time: TimeOfDay.fromJson(json['time'] as Map<String, dynamic>),
      end: json['end'] == null ? null : DateTime.parse(json['end'] as String),
      separationCount: (json['separationCount'] as num?)?.toInt() ?? 0,
      maxNumberOfSampling: (json['maxNumberOfSampling'] as num?)?.toInt(),
      dayOfWeek: (json['dayOfWeek'] as num?)?.toInt(),
      weekOfMonth: (json['weekOfMonth'] as num?)?.toInt(),
      dayOfMonth: (json['dayOfMonth'] as num?)?.toInt(),
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?
      ..period = Duration(microseconds: (json['period'] as num).toInt());

Map<String, dynamic> _$RecurrentScheduledTriggerToJson(
        RecurrentScheduledTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'type': _$RecurrentTypeEnumMap[instance.type]!,
      'time': instance.time.toJson(),
      if (instance.end?.toIso8601String() case final value?) 'end': value,
      'separationCount': instance.separationCount,
      if (instance.maxNumberOfSampling case final value?)
        'maxNumberOfSampling': value,
      if (instance.dayOfWeek case final value?) 'dayOfWeek': value,
      if (instance.weekOfMonth case final value?) 'weekOfMonth': value,
      if (instance.dayOfMonth case final value?) 'dayOfMonth': value,
      'period': instance.period.inMicroseconds,
    };

const _$RecurrentTypeEnumMap = {
  RecurrentType.daily: 'daily',
  RecurrentType.weekly: 'weekly',
  RecurrentType.monthly: 'monthly',
};

CronScheduledTrigger _$CronScheduledTriggerFromJson(
        Map<String, dynamic> json) =>
    CronScheduledTrigger()
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?
      ..cronExpression = json['cronExpression'] as String;

Map<String, dynamic> _$CronScheduledTriggerToJson(
        CronScheduledTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'cronExpression': instance.cronExpression,
    };

SamplingEventTrigger _$SamplingEventTriggerFromJson(
        Map<String, dynamic> json) =>
    SamplingEventTrigger(
      measureType: json['measureType'] as String,
      triggerCondition: json['triggerCondition'] == null
          ? null
          : Data.fromJson(json['triggerCondition'] as Map<String, dynamic>),
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$SamplingEventTriggerToJson(
        SamplingEventTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'measureType': instance.measureType,
      if (instance.triggerCondition?.toJson() case final value?)
        'triggerCondition': value,
    };

ConditionalSamplingEventTrigger _$ConditionalSamplingEventTriggerFromJson(
        Map<String, dynamic> json) =>
    ConditionalSamplingEventTrigger(
      measureType: json['measureType'] as String,
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$ConditionalSamplingEventTriggerToJson(
        ConditionalSamplingEventTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'measureType': instance.measureType,
    };

ConditionalPeriodicTrigger _$ConditionalPeriodicTriggerFromJson(
        Map<String, dynamic> json) =>
    ConditionalPeriodicTrigger(
      period: Duration(microseconds: (json['period'] as num).toInt()),
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$ConditionalPeriodicTriggerToJson(
        ConditionalPeriodicTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'period': instance.period.inMicroseconds,
    };

RandomRecurrentTrigger _$RandomRecurrentTriggerFromJson(
        Map<String, dynamic> json) =>
    RandomRecurrentTrigger(
      minNumberOfTriggers: (json['minNumberOfTriggers'] as num?)?.toInt() ?? 0,
      maxNumberOfTriggers: (json['maxNumberOfTriggers'] as num?)?.toInt() ?? 1,
      startTime: TimeOfDay.fromJson(json['startTime'] as Map<String, dynamic>),
      endTime: TimeOfDay.fromJson(json['endTime'] as Map<String, dynamic>),
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?
      ..lastTriggerTimestamp = json['lastTriggerTimestamp'] == null
          ? null
          : DateTime.parse(json['lastTriggerTimestamp'] as String);

Map<String, dynamic> _$RandomRecurrentTriggerToJson(
        RandomRecurrentTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'startTime': instance.startTime.toJson(),
      'endTime': instance.endTime.toJson(),
      'minNumberOfTriggers': instance.minNumberOfTriggers,
      'maxNumberOfTriggers': instance.maxNumberOfTriggers,
      if (instance.lastTriggerTimestamp?.toIso8601String() case final value?)
        'lastTriggerTimestamp': value,
    };

UserTaskTrigger _$UserTaskTriggerFromJson(Map<String, dynamic> json) =>
    UserTaskTrigger(
      taskName: json['taskName'] as String,
      triggerCondition:
          $enumDecode(_$UserTaskStateEnumMap, json['triggerCondition']),
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$UserTaskTriggerToJson(UserTaskTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'taskName': instance.taskName,
      'triggerCondition': _$UserTaskStateEnumMap[instance.triggerCondition]!,
    };

const _$UserTaskStateEnumMap = {
  UserTaskState.initialized: 'initialized',
  UserTaskState.enqueued: 'enqueued',
  UserTaskState.dequeued: 'dequeued',
  UserTaskState.notified: 'notified',
  UserTaskState.started: 'started',
  UserTaskState.canceled: 'canceled',
  UserTaskState.done: 'done',
  UserTaskState.expired: 'expired',
  UserTaskState.undefined: 'undefined',
};

NoUserTaskTrigger _$NoUserTaskTriggerFromJson(Map<String, dynamic> json) =>
    NoUserTaskTrigger(
      taskName: json['taskName'] as String,
    )
      ..$type = json['__type'] as String?
      ..sourceDeviceRoleName = json['sourceDeviceRoleName'] as String?;

Map<String, dynamic> _$NoUserTaskTriggerToJson(NoUserTaskTrigger instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.sourceDeviceRoleName case final value?)
        'sourceDeviceRoleName': value,
      'taskName': instance.taskName,
    };

FileData _$FileDataFromJson(Map<String, dynamic> json) => FileData(
      filename: json['filename'] as String,
      upload: json['upload'] as bool? ?? true,
    )
      ..$type = json['__type'] as String?
      ..path = json['path'] as String?
      ..metadata = (json['metadata'] as Map<String, dynamic>?)?.map(
        (k, e) => MapEntry(k, e as String),
      );

Map<String, dynamic> _$FileDataToJson(FileData instance) => <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      if (instance.path case final value?) 'path': value,
      'filename': instance.filename,
      'upload': instance.upload,
      if (instance.metadata case final value?) 'metadata': value,
    };

Heartbeat _$HeartbeatFromJson(Map<String, dynamic> json) => Heartbeat(
      period: (json['period'] as num).toInt(),
      deviceType: json['deviceType'] as String,
      deviceRoleName: json['deviceRoleName'] as String,
    )..$type = json['__type'] as String?;

Map<String, dynamic> _$HeartbeatToJson(Heartbeat instance) => <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      'period': instance.period,
      'deviceType': instance.deviceType,
      'deviceRoleName': instance.deviceRoleName,
    };

CompletedAppTask _$CompletedAppTaskFromJson(Map<String, dynamic> json) =>
    CompletedAppTask(
      taskName: json['taskName'] as String,
      taskType: json['taskType'] as String,
      taskData: json['taskData'] == null
          ? null
          : Data.fromJson(json['taskData'] as Map<String, dynamic>),
    )
      ..$type = json['__type'] as String?
      ..completedAt = DateTime.parse(json['completedAt'] as String);

Map<String, dynamic> _$CompletedAppTaskToJson(CompletedAppTask instance) =>
    <String, dynamic>{
      if (instance.$type case final value?) '__type': value,
      'taskName': instance.taskName,
      if (instance.taskData?.toJson() case final value?) 'taskData': value,
      'taskType': instance.taskType,
      'completedAt': instance.completedAt.toIso8601String(),
    };
