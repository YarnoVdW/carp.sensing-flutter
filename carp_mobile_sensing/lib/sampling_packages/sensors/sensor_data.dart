/*
 * Copyright 2018 Copenhagen Center for Health Technology (CACHET) at the
 * Technical University of Denmark (DTU).
 * Use of this source code is governed by a MIT-style license that can be
 * found in the LICENSE file.
 */
part of sensors;

/// Ambient light intensity in Lux.
/// Typically collected from the light sensor on the front of the phone.
@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class AmbientLight extends SensorData {
  static const dataType = SensorSamplingPackage.AMBIENT_LIGHT;

  num? meanLux;
  num? stdLux;
  num? minLux;
  num? maxLux;

  AmbientLight({this.meanLux, this.stdLux, this.minLux, this.maxLux}) : super();

  @override
  Function get fromJsonFunction => _$AmbientLightFromJson;
  factory AmbientLight.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as AmbientLight;
  @override
  Map<String, dynamic> toJson() => _$AmbientLightToJson(this);

  @override
  String toString() =>
      '${super.toString()}, avgLux: $meanLux, stdLux: $stdLux, minLux: $minLux, maxLux: $maxLux';
}

@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class AverageAccelerometer extends SensorData {
  static const dataType = SensorSamplingPackage.NON_GRAVITATIONAL_ACCELERATION_AVERAGE;

  double? xm;
  double? ym;
  double? zm;
  double? xms;
  double? yms;
  double? zms;
  int? n;

  AverageAccelerometer({this.xm, this.ym, this.zm, this.xms, this.yms, this.zms,this.n}) : super();

  @override
  Function get fromJsonFunction => _$AverageAccelerometerFromJson;
  factory AverageAccelerometer.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as AverageAccelerometer;
  @override
  Map<String, dynamic> toJson() => _$AverageAccelerometerToJson(this);

  @override
  String toString() =>
      '${super.toString()}, n: $n, xm: $xm, ym: $ym, zm: $zm , xms: $xms, yms: $yms, zms: $zms';
}
