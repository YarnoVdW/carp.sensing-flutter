/*
 * Copyright 2018-2020 Copenhagen Center for Health Technology (CACHET) at the
 * Technical University of Denmark (DTU).
 * Use of this source code is governed by a MIT-style license that can be
 * found in the LICENSE file.
 */

part of '../connectivity.dart';

/// The [ConnectivityProbe] listens to the connectivity status of the phone and
/// collect a [Connectivity] data point every time the connectivity state changes.
class ConnectivityProbe extends StreamProbe {
  @override
  Future<bool> onStart() async {
    // collect the current connectivity status on sampling start
    var connectivityStatus = await connectivity.Connectivity().checkConnectivity();
    addMeasurement(Measurement.fromData(Connectivity.fromConnectivityResult(connectivityStatus)));

    return super.onStart();
  }

  @override
  Stream<Measurement> get stream => connectivity.Connectivity()
      .onConnectivityChanged
      .map((event) => Measurement.fromData(Connectivity.fromConnectivityResult(event)));
}

// This probe requests access to location permissions (both on Android and iOS).
// See https://pub.dev/packages/network_info_plus
/// The [WifiProbe] get the wifi connectivity status of the phone and
/// collect a [Wifi].
///
/// Note, that in order to make this probe work on iOS (especially after iOS
/// 12 and 13), there is a set of requirements to meet for the app using this
/// probe. See
///
///  * [network_info_plus](https://pub.dev/packages/network_info_plus)
///  * [CNCopyCurrentNetworkInfo](https://developer.apple.com/documentation/systemconfiguration/1614126-cncopycurrentnetworkinfo)
///
/// Please note that it this probes does not work on emulators (returns null).
///
/// From Android 10.0 onwards the `ACCESS_FINE_LOCATION` permission must be
/// granted.
class WifiProbe extends IntervalProbe {
  @override
  Future<Measurement> getMeasurement() async {
    String? ssid = await NetworkInfo().getWifiName();
    String? bssid = await NetworkInfo().getWifiBSSID();
    String? ip = await NetworkInfo().getWifiIP();

    return Measurement.fromData(Wifi(ssid: ssid, bssid: bssid, ip: ip));
  }
}

/// The [BluetoothProbe] scans for nearby and visible Bluetooth devices and
/// collects a [Bluetooth] measurement that lists each device found during the scan.
///
/// Uses a [PeriodicSamplingConfiguration] for configuration the [interval]
/// and [duration] of the scan. Can also be configured to filter by
/// [services] and [remoteIds] by using a [BluetoothScanPeriodicSamplingConfiguration].
class BluetoothProbe extends BufferingPeriodicStreamProbe {
  /// Default timeout for bluetooth scan - 4 secs
  static const DEFAULT_TIMEOUT = 4 * 1000;
  Data? _data;

  @override
  Stream<dynamic> get bufferingStream => FlutterBluePlus.scanResults;

  @override
  Future<Measurement?> getMeasurement() async => _data != null ? Measurement.fromData(_data!) : null;

  // if a BT-specific sampling configuration is used, we need to
  // extract the services and remoteIds from it so FlutterBluePlus can
  // perform filtered scanning
  List<Guid> get services => (samplingConfiguration is BluetoothScanPeriodicSamplingConfiguration)
      ? (samplingConfiguration as BluetoothScanPeriodicSamplingConfiguration).withServices.map((e) => Guid(e)).toList()
      : [];

  List<String> get remoteIds => (samplingConfiguration is BluetoothScanPeriodicSamplingConfiguration)
      ? (samplingConfiguration as BluetoothScanPeriodicSamplingConfiguration).withRemoteIds
      : [];

  @override
  void onSamplingStart() {
    _data = Bluetooth();

    try {
      FlutterBluePlus.startScan(
        withServices: services,
        withRemoteIds: remoteIds,
        timeout: samplingConfiguration?.duration ?? const Duration(milliseconds: DEFAULT_TIMEOUT),
      );
    } catch (error) {
      FlutterBluePlus.stopScan();
      _data = Error(message: 'Error scanning for bluetooth - $error');
    }
  }

  @override
  void onSamplingEnd() {
    FlutterBluePlus.stopScan();

    if (_data is Bluetooth) (_data as Bluetooth).endScan = DateTime.now();
  }

  @override
  void onSamplingData(event) {
    if (event is List<ScanResult>) {
      (_data as Bluetooth).addBluetoothDevicesFromScanResults(event);
    }
  }
}

/// A Probe that constantly scans for nearby and visible iBeacon devices and collects a
/// [BeaconData] measurement that lists each [BeaconDevice] found during the scan.
///
/// Uses a [BeaconRangingPeriodicSamplingConfiguration] for configuration the
/// [beaconRegions] to include and the [beaconDistance].
class BeaconProbe extends StreamProbe {
  @override
  BeaconRangingPeriodicSamplingConfiguration? get samplingConfiguration =>
      super.samplingConfiguration as BeaconRangingPeriodicSamplingConfiguration;

  List<Region> get beaconRegions =>
      samplingConfiguration?.beaconRegions.map((region) => region.toRegion()).toList() ?? [];

  int get beaconDistance => samplingConfiguration?.beaconDistance ?? 2;

  List<Proximity> get includedBeaconProximities => samplingConfiguration?.includedBeaconProximities ?? [];

  @override
  bool onInitialize() {
    super.onInitialize();
    if (beaconRegions.isEmpty) {
      warning('$runtimeType - No beacon regions specified for monitoring. Will not start monitoring.');
      return false;
    }

    try {
      info('$runtimeType - Initializing iBeacon scanning...');
      flutterBeacon.initializeScanning.then((_) {
        info('$runtimeType - Initialized.');
        return true;
      }, onError: (Object error) {
        warning('$runtimeType - Error while initializing scanner - $error');
        return false;
      });
    } catch (error) {
      warning('$runtimeType - Error while initializing scanner - $error');
      return false;
    }
    return true;
  }

  @override
  Stream<Measurement> get stream async* {
    await for (final monitoringResult in flutterBeacon.monitoring(beaconRegions)) {
      if (monitoringResult.monitoringState == MonitoringState.inside) {
        debug('$runtimeType - Entered region: ${monitoringResult.region.identifier}');

        await for (final rangingResult in flutterBeacon.ranging(beaconRegions)) {
          final closeBeacons = rangingResult.beacons.where((b) => b.accuracy <= beaconDistance).toList();

          if (includedBeaconProximities.isNotEmpty) {
            closeBeacons.retainWhere((b) => includedBeaconProximities.contains(b.proximity));
          }

          if (closeBeacons.isEmpty) {
            debug('$runtimeType - No close beacons found, stopping ranging.');
            continue;
          }

          for (var beacon in closeBeacons) {
            debug('$runtimeType - Found close beacon: $beacon');
            if (beacon.proximity == Proximity.immediate) {
              debug('$runtimeType - Beacon is immediate: $beacon');
            } else if (beacon.proximity == Proximity.near) {
              debug('$runtimeType - Beacon is near: $beacon');
            }
          }

          yield Measurement.fromData(
            BeaconData.fromRegionAndBeacons(
              region: rangingResult.region.identifier,
              beacons: closeBeacons,
            ),
          );
        }
      } else if (monitoringResult.monitoringState == MonitoringState.outside) {
        debug('$runtimeType - Exited region: ${monitoringResult.region.identifier}');
      } else {
        debug('$runtimeType - Unknown state for region: ${monitoringResult.region.identifier}');
      }
    }
  }
}

/// A Probe that periodically scans for nearby and visible iBeacon devices and collects a
/// [BeaconData] measurement that lists each [BeaconDevice] found during the scan.
///
/// Uses a [BeaconRangingPeriodicSamplingConfiguration] for configuration the
/// [beaconRegions] to include and the [beaconDistance].
class BeaconPeriodicProbe extends BufferingPeriodicStreamProbe {
  @override
  BeaconPeriodicSamplingConfiguration? get samplingConfiguration =>
      super.samplingConfiguration as BeaconPeriodicSamplingConfiguration;

  List<Region> get beaconRegions =>
      samplingConfiguration?.beaconRegions.map((region) => region.toRegion()).toList() ?? [];

  int get beaconDistance => samplingConfiguration?.beaconDistance ?? 2;

  List<Proximity> get includedBeaconProximities => samplingConfiguration?.includedBeaconProximities ?? [];

  Data? _data;

  StreamSubscription<MonitoringResult>? _streamMonitoring;
  StreamSubscription<RangingResult>? _streamRanging;

  @override
  Stream<dynamic> get bufferingStream async* {
    await for (final monitoringResult in flutterBeacon.monitoring(beaconRegions)) {
      if (monitoringResult.monitoringState == MonitoringState.inside) {
        print('$runtimeType - Entered region: ${monitoringResult.region.identifier}');

        await for (final rangingResult in flutterBeacon.ranging(beaconRegions)) {
          final closeBeacons = rangingResult.beacons.where((b) => b.accuracy <= beaconDistance).toList();

          if (includedBeaconProximities.isNotEmpty) {
            closeBeacons.retainWhere((b) => includedBeaconProximities.contains(b.proximity));
          }

          if (closeBeacons.isEmpty) {
            print('$runtimeType - No close beacons found, stopping ranging.');
            continue;
          }

          for (var beacon in closeBeacons) {
            print('$runtimeType - Found close beacon: $beacon');
            if (beacon.proximity == Proximity.immediate) {
              print('$runtimeType - Beacon is immediate: $beacon');
            } else if (beacon.proximity == Proximity.near) {
              print('$runtimeType - Beacon is near: $beacon');
            }
          }

          yield closeBeacons;
        }
      } else if (monitoringResult.monitoringState == MonitoringState.outside) {
        print('$runtimeType - Exited region: ${monitoringResult.region.identifier}');
      } else {
        print('$runtimeType - Unknown state for region: ${monitoringResult.region.identifier}');
      }
    }
  }

  @override
  Future<Measurement?> getMeasurement() async => _data != null ? Measurement.fromData(_data!) : null;

  @override
  void onSamplingStart() {
    _data = BeaconData(region: '');
    try {
      print('Using beacon monitoring.');
      _startMonitoring();
    } catch (error) {
      _data = Error(message: 'Error scanning for bluetooth - $error');
    }
  }

  @override
  void onSamplingEnd() {
    info('stopping monitoring kinda');
    _stopMonitoring();
  }

  @override
  void onSamplingData(event) {
    if (event is RangingResult) {
      (_data as BeaconData).addBeaconDevicesFromRangingResults(event);
    }
  }

  Future<void> _startMonitoring() async {
    print('start monitoring & initializing scanning.');
    try {
      await flutterBeacon.initializeScanning;
    } catch (e) {
      warning('error happened while initializing scanner $e');
    }
    print('initialized scanner');

    List<Region> regions = beaconRegions.isEmpty ? [] : beaconRegions.map((beaconRegion) => beaconRegion).toList();

    try {
      _streamMonitoring = flutterBeacon.monitoring(regions).listen((MonitoringResult result) {
        if (result.monitoringState == MonitoringState.inside) {
          print('🚪 Entered region: ${result.region.identifier}');
          _startRanging(result.region);
        } else if (result.monitoringState == MonitoringState.outside ||
            result.monitoringState == MonitoringState.unknown) {
          print('🚪 Exited region: ${result.region.identifier}');
          _stopMonitoring();
        }
      });
    } catch (e) {
      print('Error starting monitoring: $e');
    }
  }

  void _startRanging(Region region) {
    _streamRanging = flutterBeacon.ranging([region]).listen((RangingResult result) {
      final closeBeacons = result.beacons.where((beacon) => (beacon.accuracy).abs() <= beaconDistance);

      for (var beacon in closeBeacons) {
        print('✅ beacon in range: ${region.proximityUUID}, ${beacon.accuracy} m');
        (_data as BeaconData).addBeaconDevicesFromRangingResults(result);
      }
    });
  }

  void _stopMonitoring() {
    print('Stopping monitoring.');
    _streamRanging?.cancel();
    _streamRanging = null;
    _streamMonitoring?.cancel();
    _streamMonitoring = null;
  }
}
