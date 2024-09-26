/*
 * Copyright 2022 Copenhagen Center for Health Technology (CACHET) at the
 * Technical University of Denmark (DTU).
 * Use of this source code is governed by a MIT-style license that can be
 * found in the LICENSE file.
 */

part of '../../carp_context_package.dart';

/// An [OnlineService] for the [Open Weather](https://openweathermap.org/) service.
@JsonSerializable(includeIfNull: false, explicitToJson: true)
class WeatherService extends OnlineService {
  /// The type of a air quality service.
  static const String DEVICE_TYPE =
      '${DeviceConfiguration.DEVICE_NAMESPACE}.WeatherService';

  /// The default role name for a weather service.
  static const String DEFAULT_ROLE_NAME = 'Weather Service';

  /// API key for the Open Weather API.
  String apiKey;

  WeatherService({
    String? roleName,
    required this.apiKey,
  }) : super(
          roleName: roleName ?? DEFAULT_ROLE_NAME,
        );

  @override
  Function get fromJsonFunction => _$WeatherServiceFromJson;
  factory WeatherService.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson<WeatherService>(json);
  @override
  Map<String, dynamic> toJson() => _$WeatherServiceToJson(this);
}

/// A [DeviceManager] for the [WeatherService].
class WeatherServiceManager extends OnlineServiceManager<WeatherService> {
  weather.WeatherFactory? _service;

  /// A handle to the [WeatherFactory] plugin.
  /// Returns null if the service is not configured.
  weather.WeatherFactory? get service => (_service != null)
      ? _service
      : (configuration?.apiKey != null)
          ? _service = weather.WeatherFactory(configuration!.apiKey)
          : null;

  @override
  String get id => configuration?.apiKey ?? 'N/A';

  @override
  String? get displayName => 'Weather Service (OW)';

  WeatherServiceManager([
    WeatherService? configuration,
  ]) : super(WeatherService.DEVICE_TYPE, configuration);

  @override
  Future<bool> onHasPermissions() async =>
      (await Permission.locationWhenInUse.status).isGranted;

  @override
  Future<void> onRequestPermissions() async =>
      await LocationManager().requestPermission();

  @override
  // ignore: avoid_renaming_method_parameters
  void onInitialize(WeatherService service) {}

  @override
  Future<bool> canConnect() async {
    try {
      var data = await service?.currentWeatherByLocation(
          40.63047005003576, -74.12938368359374);
      return (data != null);
    } catch (_) {
      return false;
    }
  }

  @override
  Future<DeviceStatus> onConnect() async =>
      (service != null) ? DeviceStatus.connected : DeviceStatus.disconnected;

  @override
  Future<bool> onDisconnect() async => true;
}
