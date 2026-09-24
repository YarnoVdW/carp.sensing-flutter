part of '../../carp_context_package.dart';

/// Collects local weather information using the [WeatherServiceManager].
class WeatherProbe extends MeasurementProbe {
  @override
  WeatherServiceManager get deviceManager =>
      super.deviceManager as WeatherServiceManager;

  @override
  bool onInitialize() {
    LocationManager().enable().then((_) => super.onInitialize());
    return true;
  }

  /// Returns the [Weather] for this location wrapped as a [Measurement].
  @override
  Future<Measurement> getMeasurement() async {
    if (deviceManager.service != null) {
      bool serviceEnabled;
      LocationPermission permission;

      try {
        serviceEnabled = await Geolocator.isLocationServiceEnabled();
        if (!serviceEnabled) {
          warning(
              '$runtimeType - Location service is not enabled. Cannot get weather.');
          return Measurement.fromData(Error(
              message:
                  '$runtimeType - Location service is not enabled in system settings. Cannot get weather.'));
        }

        permission = await Geolocator.checkPermission();
        if (permission == LocationPermission.denied) {
          permission = await Geolocator.requestPermission();
          if (permission == LocationPermission.denied) {
            warning(
                '$runtimeType - Location service is not enabled. Cannot get weather.');
            return Measurement.fromData(Error(
                message:
                    '$runtimeType - Some permissions are not granted. Cannot get weather.'));
          }
        }

        if (permission == LocationPermission.deniedForever) {
          warning(
              '$runtimeType - Location service is not enabled. Cannot get weather.');
          return Measurement.fromData(Error(
              message:
                  '$runtimeType - Some permissions are permanently denied. Cannot get weather.'));
        }

        final loc = await Geolocator.getCurrentPosition();
        final w = await deviceManager.service!.currentWeatherByLocation(
          loc.latitude,
          loc.longitude,
        );
        return Measurement.fromData(Weather.fromWeatherData(w));
      } catch (error) {
        warning('$runtimeType - Error getting weather - $error');
        return Measurement.fromData(
            Error(message: '$runtimeType Exception: $error'));
      }
    }
    warning(
        '$runtimeType - no service available. Check if the WeatherService has been added to the study protocol?');
    return Measurement.fromData(
        Error(message: '$runtimeType - no service available.'));
  }
}
