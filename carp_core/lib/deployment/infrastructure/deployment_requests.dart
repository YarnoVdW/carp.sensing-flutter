/*
 * Copyright 2020 Copenhagen Center for Health Technology (CACHET) at the
 * Technical University of Denmark (DTU).
 * Use of this source code is governed by a MIT-style license that can be
 * found in the LICENSE file.
 */
part of '../carp_core_deployment.dart';

// -----------------------------------------------------
// Deployment Service Requests
// See https://github.com/cph-cachet/carp.core-kotlin/blob/develop/carp.deployment.core/src/commonMain/kotlin/dk/cachet/carp/deployment/infrastructure/DeploymentServiceRequest.kt
// -----------------------------------------------------

/// A [DeploymentServiceRequest] and all its sub-classes contain the data for
/// sending a RPC request to the CARP web service.
///
/// All deployment requests to the CARP Service is defined in
/// [carp.core-kotlin](https://github.com/cph-cachet/carp.core-kotlin/blob/develop/carp.deployment.core/src/commonMain/kotlin/dk/cachet/carp/deployment/infrastructure/DeploymentServiceRequest.kt)
abstract class DeploymentServiceRequest extends ServiceRequest {
  final String _infrastructurePackageNamespace =
      'dk.cachet.carp.deployments.infrastructure';

  DeploymentServiceRequest([this.studyDeploymentId]) : super();

  @override
  String get apiVersion => DeploymentService.API_VERSION;

  /// The CARP study deployment ID.
  String? studyDeploymentId;

  @override
  String get jsonType =>
      '$_infrastructurePackageNamespace.DeploymentServiceRequest.$runtimeType';

  @override
  String toString() => '$runtimeType - studyDeploymentId: $studyDeploymentId';
}

/// A request for creating a deployment based on the [protocol].
@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class CreateStudyDeployment extends DeploymentServiceRequest {
  StudyProtocol protocol;
  List<ParticipantInvitation> invitations;
  Map<String, DeviceRegistration>? connectedDevicePreregistrations;

  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  // ignore: overridden_fields
  String? studyDeploymentId;

  CreateStudyDeployment(
    this.protocol,
    this.invitations, [
    this.connectedDevicePreregistrations,
  ]) : super();

  @override
  Function get fromJsonFunction => _$CreateStudyDeploymentFromJson;
  factory CreateStudyDeployment.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as CreateStudyDeployment;
  @override
  Map<String, dynamic> toJson() => _$CreateStudyDeploymentToJson(this);

  @override
  String toString() => '$runtimeType - protocol: ${protocol.name}}';
}

/// A request for getting the status of a study deployment.
@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class GetStudyDeploymentStatus extends DeploymentServiceRequest {
  GetStudyDeploymentStatus(super.studyDeploymentId);

  @override
  Function get fromJsonFunction => _$GetStudyDeploymentStatusFromJson;
  factory GetStudyDeploymentStatus.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as GetStudyDeploymentStatus;
  @override
  Map<String, dynamic> toJson() => _$GetStudyDeploymentStatusToJson(this);
}

/// A request for getting the status of a list of study deployment.
@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class GetStudyDeploymentStatusList extends DeploymentServiceRequest {
  List<String> studyDeploymentIds;

  GetStudyDeploymentStatusList(this.studyDeploymentIds) : super();

  @override
  Function get fromJsonFunction => _$GetStudyDeploymentStatusListFromJson;
  factory GetStudyDeploymentStatusList.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as GetStudyDeploymentStatusList;
  @override
  Map<String, dynamic> toJson() => _$GetStudyDeploymentStatusListToJson(this);
}

/// A request for registering this device.
@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class RegisterDevice extends DeploymentServiceRequest {
  RegisterDevice(
      super.studyDeploymentId, this.deviceRoleName, this.registration);

  /// The role name of this device.
  String deviceRoleName;

  /// The registration.
  DeviceRegistration registration;

  @override
  Function get fromJsonFunction => _$RegisterDeviceFromJson;
  factory RegisterDevice.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as RegisterDevice;
  @override
  Map<String, dynamic> toJson() => _$RegisterDeviceToJson(this);

  @override
  String toString() => '${super.toString()}, deviceRoleName: $deviceRoleName';
}

/// A request for unregistering this device.
@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class UnregisterDevice extends DeploymentServiceRequest {
  UnregisterDevice(super.studyDeploymentId, this.deviceRoleName);

  /// The role name of this device.
  String deviceRoleName;

  @override
  Function get fromJsonFunction => _$UnregisterDeviceFromJson;
  factory UnregisterDevice.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as UnregisterDevice;
  @override
  Map<String, dynamic> toJson() => _$UnregisterDeviceToJson(this);

  @override
  String toString() => '${super.toString()}, deviceRoleName: $deviceRoleName';
}

/// A request for getting the deployment for this primary device.
@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class GetDeviceDeploymentFor extends DeploymentServiceRequest {
  GetDeviceDeploymentFor(super.studyDeploymentId, this.primaryDeviceRoleName);

  /// The role name of this primary device.
  String primaryDeviceRoleName;

  @override
  Function get fromJsonFunction => _$GetDeviceDeploymentForFromJson;
  factory GetDeviceDeploymentFor.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as GetDeviceDeploymentFor;
  @override
  Map<String, dynamic> toJson() => _$GetDeviceDeploymentForToJson(this);

  @override
  String toString() =>
      '${super.toString()}, primaryDeviceRoleName: $primaryDeviceRoleName';
}

/// A request for reporting this deployment as successful.
@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class DeviceDeployed extends GetDeviceDeploymentFor {
  /// Timestamp when this was last updated in UTC
  DateTime deviceDeploymentLastUpdatedOn;

  DeviceDeployed(
    super.studyDeploymentId,
    super.primaryDeviceRoleName,
    this.deviceDeploymentLastUpdatedOn,
  );

  @override
  Function get fromJsonFunction => _$DeviceDeployedFromJson;
  factory DeviceDeployed.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as DeviceDeployed;
  @override
  Map<String, dynamic> toJson() => _$DeviceDeployedToJson(this);

  @override
  String toString() =>
      '${super.toString()}, primaryDeviceRoleName: $primaryDeviceRoleName';
}

/// A request for permanently stopping a study deployment.
@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class Stop extends DeploymentServiceRequest {
  Stop(super.studyDeploymentId);

  @override
  Function get fromJsonFunction => _$StopFromJson;
  factory Stop.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as Stop;
  @override
  Map<String, dynamic> toJson() => _$StopToJson(this);
}
