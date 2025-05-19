part of '../services.dart';

/// Misc. settings for CAMS.
///
/// This class is a singleton, access using `Settings()`.
/// Must be initialized using the [init] method before used.
///
/// Supports:
///  * setting debug level - see [debugLevel]
///  * setting whether to save [AppTask]s across app re-start - see [saveAppTaskQueue]
///  * getting shared preferences - see [preferences]
///  * getting app info - see [packageInfo]
///  * generating a unique and anonymous user id - see [userId]
///  * getting the timezone of the app - see [timezone]
///
class Settings {
  static const String USER_ID_KEY = 'user_id';

  static const String CARP_DEPLOYMENT_FILE_PATH = 'deployments';
  static const String CARP_DATA_FILE_PATH = 'data';
  static const String CARP_CACHE_FILE_PATH = 'cache';

  static final Settings _instance = Settings._();
  factory Settings() => _instance;
  Settings._();

  SharedPreferences? _preferences;
  PackageInfo? _packageInfo;
  String? _appName;
  String? _packageName;
  String? _version;
  String? _buildNumber;
  String? _localApplicationPath, _carpBasePath;
  final Map<String, String> _deploymentBasePaths = {};
  String _timezone = 'Europe/Copenhagen';
  bool _initialized = false;

  /// Has the setting been initialized via calling the [init] method?
  bool get initialized => _initialized;

  /// The global debug level setting.
  ///
  /// See [DebugLevel] for valid debug level settings.
  /// Can be changed on runtime.
  DebugLevel debugLevel = DebugLevel.warning;

  /// Save the queue of [AppTask]s in the [AppTaskController] across
  /// app re-start?
  bool saveAppTaskQueue = true;

  /// The app name.
  /// `CFBundleDisplayName` on iOS, `application/label` on Android.
  String? get appName => _appName;

  /// The package name.
  /// `bundleIdentifier` on iOS, `getPackageName` on Android.
  String? get packageName => _packageName;

  /// The package version.
  /// `CFBundleShortVersionString` on iOS, `versionName` on Android.
  String? get version => _version;

  /// The build number.
  /// `CFBundleVersion` on iOS, `versionCode` on Android.
  String? get buildNumber => _buildNumber;

  /// A simple persistent store for simple data. Note that data is saved in
  /// plain format and should hence **not** be used for sensitive data.
  ///
  /// Wraps NSUserDefaults (on iOS) and SharedPreferences (on Android).
  SharedPreferences? get preferences => _preferences;

  /// Package information
  PackageInfo? get packageInfo => _packageInfo;

  /// Path to a directory where the application may place data that is
  /// user-generated.
  Future<String> get localApplicationPath async {
    if (_localApplicationPath == null) {
      final directory = await getApplicationDocumentsDirectory();
      _localApplicationPath = directory.path;
    }
    return _localApplicationPath!;
  }

  /// The base path for storing all CARP related files on the form
  ///
  ///  `<localApplicationPath>/carp`
  ///
  Future<String> get carpBasePath async {
    if (_carpBasePath == null) {
      _carpBasePath = '${await localApplicationPath}/carp';
      var directory = Directory(_carpBasePath!);
      await directory.exists().then((exists) {
        if (!exists) directory.createSync(recursive: true);
      });
    }

    return _carpBasePath!;
  }

  /// The base path for storing all deployment related files on the form
  ///
  ///  `<localApplicationPath>/carp/deployments/<study_deployment_id>`
  ///
  Future<String> getDeploymentBasePath(String studyDeploymentId) async {
    if (_deploymentBasePaths[studyDeploymentId] == null) {
      final path =
          '${await carpBasePath}/$CARP_DEPLOYMENT_FILE_PATH/$studyDeploymentId';
      _deploymentBasePaths[studyDeploymentId] = path;
      Directory(path).createSync(recursive: true);
    }

    return _deploymentBasePaths[studyDeploymentId]!;
  }

  /// The base path for storing all cached data.
  ///
  ///  `<localApplicationPath>/carp/deployments/<study_deployment_id>/cache`
  ///
  Future<
      String> getCacheBasePath(String studyDeploymentId) async => (await Directory(
              '${await getDeploymentBasePath(studyDeploymentId)}/$CARP_CACHE_FILE_PATH')
          .create(recursive: true))
      .path;

  /// The base path for storing all data (e.g. media files).
  ///
  ///  `<localApplicationPath>/carp/deployments/<study_deployment_id>/data`
  ///
  Future<
      String> getDataBasePath(String studyDeploymentId) async => (await Directory(
              '${await getDeploymentBasePath(studyDeploymentId)}/$CARP_DATA_FILE_PATH')
          .create(recursive: true))
      .path;

  // /// The base path for storing all cached data.
  // ///
  // ///  `<localApplicationPath>/carp/deployments/<study_deployment_id>/cache`
  // ///
  // Future<String> getCacheBasePath(String studyDeploymentId) async {
  //   var cachePath =
  //       '${await getDeploymentBasePath(studyDeploymentId)}/$CARP_CACHE_FILE_PATH';

  //   var directory = Directory(cachePath);
  //   await directory.exists().then((exists) {
  //     debug('$runtimeType - Creating CACHE path: $cachePath');
  //     if (!exists) directory.createSync(recursive: true);
  //   });

  //   return cachePath;
  // }

  // /// The base path for storing all data (e.g. media files).
  // ///
  // ///  `<localApplicationPath>/carp/deployments/<study_deployment_id>/data`
  // ///
  // Future<String> getDataBasePath(String studyDeploymentId) async {
  //   var dataPath =
  //       '${await getDeploymentBasePath(studyDeploymentId)}/$CARP_DATA_FILE_PATH';

  //   var directory = Directory(dataPath);
  //   await directory.exists().then((exists) {
  //     if (!exists) directory.createSync(recursive: true);
  //   });

  //   return dataPath;
  // }

  /// The local time zone setting of this app.
  String get timezone => _timezone;

  /// Initialize settings. Must be called before using any settings.
  Future<void> init() async {
    if (_initialized) return;
    _initialized = true;

    _preferences ??= await SharedPreferences.getInstance();
    _packageInfo ??= await PackageInfo.fromPlatform();

    _appName = _packageInfo!.appName;
    _packageName = _packageInfo!.packageName;
    _version = _packageInfo!.version;
    _buildNumber = _packageInfo!.buildNumber;

    await localApplicationPath.then((_) => carpBasePath);

    debug('$runtimeType - Shared Preferences:');
    _preferences!
        .getKeys()
        .forEach((key) => debug('[$key] : ${_preferences!.get(key)}'));

    // setting up time zone settings
    tz.initializeTimeZones();
    try {
      _timezone = await FlutterTimezone.getLocalTimezone();
    } catch (_) {
      _timezone = tz.local.name;
      warning(
          'Could not get the local timezone - setting timezone to $timezone');
    }
    info('Time zone set to $timezone');
    info('$runtimeType initialized');
  }

  String? _userId;

  /// Generate a user id that is;
  ///  * unique
  ///  * anonymous
  ///  * persistent
  ///
  /// This id is generated the first time this method is called and then stored
  /// on the phone in-between sessions, and will therefore be the same for
  /// the same app on the same phone.
  Future<String> get userId async {
    assert(_preferences != null,
        "Setting is not initialized. Call 'Setting().init()' first.");
    if (_userId == null) {
      _userId = preferences!.get(USER_ID_KEY) as String?;
      if (_userId == null) {
        _userId = const Uuid().v1;
        await preferences!.setString(USER_ID_KEY, _userId!);
      }
    }
    return _userId!;
  }
}

/// Debugging levels.
enum DebugLevel { none, info, warning, debug }
