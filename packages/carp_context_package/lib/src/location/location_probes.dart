/*
 * Copyright 2018 Copenhagen Center for Health Technology (CACHET) at the
 * Technical University of Denmark (DTU).
 * Use of this source code is governed by a MIT-style license that can be
 * found in the LICENSE file.
 */

part of '../../carp_context_package.dart';

/// Collects streaming location information from the underlying OS's location API.
/// Is a [StreamProbe] that generates a [Location] data point every time
/// location is changed.
class LocationProbe extends StreamProbe {
  @override
  LocationServiceManager get deviceManager =>
      super.deviceManager as LocationServiceManager;

  @override
  Stream<Measurement> get stream => deviceManager.manager.onLocationChanged
      .map((location) => Measurement.fromData(location));
}

/// A probe that collects location data from the underlying OS's location API.
///
/// This probe can be configured to collect location data continuously (default)
/// or only once using a [LocationSamplingConfiguration].
/// If the configuration is set to collect location data only once, the probe
/// will automatically stop after collecting one location.
class ConfigurableLocationProbe extends Probe {
  LocationSamplingConfiguration? _configuration;
  StreamSubscription<Measurement>? _subscription;

  bool get oneTimeSampling => _configuration?.once ?? false;
  LocationService? get settings => deviceManager.configuration;

  @override
  LocationServiceManager get deviceManager =>
      super.deviceManager as LocationServiceManager;

  @override
  bool onInitialize() {
    if (samplingConfiguration is LocationSamplingConfiguration) {
      _configuration = samplingConfiguration as LocationSamplingConfiguration;
    }

    return super.onInitialize();
  }

  Future<bg.State>? _backgroundReady;

  Future<void> _readyBackgroundLocation() async {
    await bg.BackgroundGeolocation.registerHeadlessTask(
        backgroundLocationHeadlessTask);
    await (_backgroundReady ??= bg.BackgroundGeolocation.ready(
      bg.Config(
        app: bg.AppConfig(
          stopOnTerminate: false,
          startOnBoot: true,
          enableHeadless: true,
          heartbeatInterval: settings?.interval.inSeconds.toDouble(),
          notification: bg.Notification(
            title: settings?.notificationTitle ?? 'Location Service',
            text: settings?.notificationMessage ??
                'Location service is running in the background',
            sticky: true,
          ),
        ),
        persistence: const bg.PersistenceConfig(
          maxDaysToPersist: 30,
          persistMode: bg.PersistMode.location,
          disableProviderChangeRecord: true,
        ),
        logger: const bg.LoggerConfig(
          debug: false,
          logLevel: bg.LogLevel.verbose,
        ),
        activity: const bg.ActivityConfig(
          activityRecognitionInterval: 10000.0,
          disableStopDetection: false,
          stopOnStationary: false,
        ),
        geolocation: bg.GeoConfig(
          distanceFilter: settings?.distance ?? 0,
          locationUpdateInterval: settings?.interval.inMilliseconds ?? 0,
          fastestLocationUpdateInterval: 1000,
          deferTime: 0,
          disableElasticity: true,
          stopTimeout: 2,
          useSignificantChangesOnly: false,
          allowIdenticalLocations: true,
          desiredAccuracy: bg.DesiredAccuracy.high,
        ),
      ),
    ));
  }

  Stream<Location> _backgroundLocations() {
    late StreamController<Location> controller;
    void onLocation(bg.Location location) =>
        controller.add(_toLocation(location));
    void onHeartbeat(bg.HeartbeatEvent _) => _recordHeartbeatLocation();

    controller = StreamController<Location>(
      onListen: () {
        bg.BackgroundGeolocation.onLocation(onLocation);
        bg.BackgroundGeolocation.onHeartbeat(onHeartbeat);
      },
      onCancel: () {
        bg.BackgroundGeolocation.removeListener(onLocation);
        bg.BackgroundGeolocation.removeListener(onHeartbeat);
      },
    );
    return controller.stream;
  }

  Location _toLocation(bg.Location location) {
    double? valid(double value) => value >= 0 ? value : null;
    final coords = location.coords;
    return Location(
      latitude: coords.latitude,
      longitude: coords.longitude,
      altitude: coords.altitude,
      accuracy: valid(coords.accuracy),
      verticalAccuracy: valid(coords.altitudeAccuracy),
      speed: valid(coords.speed),
      speedAccuracy: valid(coords.speedAccuracy),
      heading: valid(coords.heading),
      headingAccuracy: valid(coords.headingAccuracy),
      time: DateTime.tryParse('${location.timestamp}'),
      isMock: location.mock,
    );
  }

  @override
  Future<bool> onStart() async {
    if (await requestPermissions()) {
      await _readyBackgroundLocation();
      // if this is a one-time sampling, just get the location once and return
      if (oneTimeSampling) {
        try {
          final location =
              await bg.BackgroundGeolocation.getCurrentPosition(samples: 1);
          addMeasurement(Measurement.fromData(_toLocation(location)));
        } catch (error) {
          warning('$runtimeType - Error getting location - $error');
          addError('$runtimeType - Error getting location: $error');
        }
        // automatically stop this probe after it is done collecting the measurement
        Future.delayed(const Duration(seconds: 5), () => stop());
      } else {
        var stream = _backgroundLocations()
            .map((location) => Measurement.fromData(location));

        _subscription = stream.listen(
          (measurement) => addMeasurement(measurement),
          onError: (Object error) => addError(error),
        );

        if (!(await bg.BackgroundGeolocation.state).enabled) {
          await bg.BackgroundGeolocation.start();
        }
      }
    }
    return true;
  }

  @override
  Future<bool> onStop() async {
    await _subscription?.cancel();
    if (!oneTimeSampling) await bg.BackgroundGeolocation.stop();
    return true;
  }

  @override
  Future<bool> onRestart() async {
    await _subscription?.cancel();
    return true;
  }
}

@pragma('vm:entry-point')
Future<void> backgroundLocationHeadlessTask(bg.HeadlessEvent event) async {
  if (event.name == bg.Event.HEARTBEAT) await _recordHeartbeatLocation();
}

Future<void> _recordHeartbeatLocation() async {
  int? taskId;
  try {
    await bg.BackgroundGeolocation.getCurrentPosition(
        samples: 1, persist: true);

    taskId = await bg.BackgroundGeolocation.startBackgroundTask();

    await Future<void>.delayed(const Duration(seconds: 45));
  } catch (error) {
    warning('Background location - Error getting heartbeat location - $error');
  } finally {
    if (taskId != null) {
      await bg.BackgroundGeolocation.stopBackgroundTask(taskId);
    }
  }
}
