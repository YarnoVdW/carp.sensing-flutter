# CARP Mobile Sensing Framework in Flutter

[![pub package](https://img.shields.io/pub/v/carp_mobile_sensing.svg)](https://pub.dartlang.org/packages/carp_mobile_sensing)
[![pub points](https://img.shields.io/pub/points/carp_mobile_sensing?color=2E8B57&label=pub%20points)](https://pub.dev/packages/carp_mobile_sensing/score)
[![github stars](https://img.shields.io/github/stars/cph-cachet/carp.sensing-flutter.svg?style=flat&logo=github&colorB=deeppink&label=stars)](https://github.com/cph-cachet/carp.sensing-flutter)
[![MIT License](https://img.shields.io/badge/license-MIT-purple.svg)](https://opensource.org/licenses/MIT)
[![arXiv](https://img.shields.io/badge/arXiv-2006.11904-green.svg)](https://arxiv.org/abs/2006.11904)

This library contains the core Flutter package for the [CARP Mobile Sensing (CAMS)](https://carp.cachet.dk/cams/) framework.
Supports cross-platform (iOS and Android) mobile sensing.

For an overview of all CAMS packages, see [CARP Mobile Sensing in Flutter](https://github.com/cph-cachet/carp.sensing-flutter).
For documentation on how to use CAMS, see the [CAMS wiki][wiki].

## Usage

To use this plugin, add [`carp_mobile_sensing`](https://pub.dev/packages/carp_mobile_sensing) as [dependencies in your `pubspec.yaml` file](https://flutter.io/platform-plugins/).

`````yaml
dependencies:
  flutter:
    sdk: flutter
  carp_core: ^latest
  carp_mobile_sensing: ^latest
`````

## Configuration

When you want to add CAMS to you app, there are a few things to do in terms of configuring your app.

First, CAMS rely on the [flutter_local_notifications](https://pub.dev/packages/flutter_local_notifications) plugin. So **if you want to use App Tasks and notifications** you should configure your app to the [platforms it supports](https://pub.dev/packages/flutter_local_notifications#-supported-platforms) and configure your app for both [Android](https://pub.dev/packages/flutter_local_notifications#-android-setup) and [iOS](https://pub.dev/packages/flutter_local_notifications#-ios-setup). There is a lot of details in configuring for notifications - especially for Android - so read this carefully.

### Android Integration

Set the minimum android SDK to 26 and Java SDK Version to 34 by setting the `minSdkVersion`, the `compileSdkVersion`, and `targetSdkVersion` in the `build.gradle` file, located in the `android/app/` folder:

```gradle
android {
    compileSdkVersion 34

    compileOptions {
        sourceCompatibility JavaVersion.VERSION_1_8
        targetCompatibility JavaVersion.VERSION_1_8
    }

    defaultConfig {
        ...
        minSdkVersion 26
        targetSdkVersion flutter.targetSdkVersion
        ...
    }
    ...
}
```

The pedometer (step count) probe needs permission to `ACTIVITY_RECOGNITION`.
Scheduled notifications (if using `AppTask`) needs a set of permissions, such as `SCHEDULE_EXACT_ALARM` and `VIBRATE`.
If collecting step counts or using notifications in your app, add the following to your app's `manifest.xml` file located in `android/app/src/main`:

````xml
<!-- Used for activity recognition (step count) -->
<uses-permission android:name="android.permission.ACTIVITY_RECOGNITION"/>

<!-- Used for sending and scheduling notifications -->
<uses-permission android:name="android.permission.FOREGROUND_SERVICE" />
<uses-permission android:name="android.permission.RECEIVE_BOOT_COMPLETED"/>
<uses-permission android:name="android.permission.VIBRATE" />
<uses-permission android:name="android.permission.POST_NOTIFICATIONS"/>
<uses-permission android:name="android.permission.SCHEDULE_EXACT_ALARM"
        android:maxSdkVersion="32" />
````

Also specify the following between the `<application>` tags so that the plugin can show the scheduled notifications:

```xml
<!-- Used for scheduling notifications -->
<receiver android:exported="false" android:name="com.dexterous.flutterlocalnotifications.ScheduledNotificationReceiver" />
<receiver android:exported="false" android:name="com.dexterous.flutterlocalnotifications.ScheduledNotificationBootReceiver">
    <intent-filter>
        <action android:name="android.intent.action.BOOT_COMPLETED"/>
        <action android:name="android.intent.action.MY_PACKAGE_REPLACED"/>
        <action android:name="android.intent.action.QUICKBOOT_POWERON" />
        <action android:name="com.htc.intent.action.QUICKBOOT_POWERON"/>
    </intent-filter>
</receiver>
```

### iOS Integration

The pedometer (step count) probe uses `NSMotion` on iOS and the `NSMotionUsageDescription` needs to be specified in the app's `Info.plist` file located in `ios/Runner`:

```xml
<key>NSMotionUsageDescription</key>
<string>Collecting step count.</string>
```

-------------------------------------

> **NOTE:** Other CAMS sampling packages require additional permissions in the `manifest.xml` or `Info.plist` files.
> See the documentation for each package.

## Documentation

The [Dart API doc](https://pub.dartlang.org/documentation/carp_mobile_sensing/latest/) describes the different libraries and classes.

The CAMS [wiki][wiki] contains detailed documentation on the CARP Mobile Sensing Framework, including
the [domain model](https://github.com/cph-cachet/carp.sensing-flutter/wiki/2.-Domain-Model),
how to use it by create a [study configuration](https://github.com/cph-cachet/carp.sensing-flutter/wiki/3.-Using-CARP-Mobile-Sensing),
how to [extend](https://github.com/cph-cachet/carp.sensing-flutter/wiki/5.-Extending-CARP-Mobile-Sensing) it, and
an overview of the available [measure types](https://github.com/cph-cachet/carp.sensing-flutter/wiki/A.-Measure-Types).

More scientific documentation of CAMS is available in the following papers:

* Bardram, Jakob E. "[The CARP Mobile Sensing Framework--A Cross-platform, Reactive, Programming Framework and Runtime Environment for Digital Phenotyping.](https://arxiv.org/abs/2006.11904)" arXiv preprint arXiv:2006.11904 (2020). [[pdf](https://arxiv.org/pdf/2006.11904.pdf)]
* Bardram, Jakob E. "[Software Architecture Patterns for Extending Sensing Capabilities and Data Formatting in Mobile Sensing.](https://www.mdpi.com/1424-8220/22/7/2813)" Sensors 22.7 (2022). [[pdf]](https://www.mdpi.com/1424-8220/22/7/2813/pdf).

```latex
@article{bardram2020carp,
  title={The CARP Mobile Sensing Framework--A Cross-platform, Reactive, Programming Framework and Runtime Environment for Digital Phenotyping},
  author={Bardram, Jakob E},
  journal={arXiv preprint arXiv:2006.11904},
  year={2020}
}

@article{bardram2022software,
  title={Software Architecture Patterns for Extending Sensing Capabilities and Data Formatting in Mobile Sensing},
  author={Bardram, Jakob E},
  journal={Sensors},
  volume={22},
  number={7},
  year={2022},
  publisher={MDPI}
}
```

Please use these references in any scientific papers using CAMS.

## Examples of configuring and using CAMS

There is a **very simple** [example app](https://github.com/cph-cachet/carp.sensing-flutter/blob/master/carp_mobile_sensing/example/lib/main.dart) app which shows how a study can be created with different tasks and measures.
This app just prints the sensing data to a console screen on the phone.
There is also a range of different [examples](https://github.com/cph-cachet/carp.sensing-flutter/blob/master/carp_mobile_sensing/example/lib/example.dart) on how to create a study to take inspiration from.

However, the [CARP Mobile Sensing App](https://github.com/cph-cachet/carp.sensing-flutter/tree/master/apps/carp_mobile_sensing_app) provides a **MUCH** better example of how to use the framework in a Flutter BLoC architecture, including good documentation of how to do this.

Below is a small primer in the use of CAMS for a very simple sampling study running locally on the phone. This example is similar to the [example app](https://github.com/cph-cachet/carp.sensing-flutter/blob/master/carp_mobile_sensing/example/lib/main.dart) app.

Following [`carp_core`](https://pub.dev/documentation/carp_core/latest/), a CAMS study can be configured, deployed, executed, and used in different steps:

1. Define a [`SmartphoneStudyProtocol`](https://pub.dev/documentation/carp_mobile_sensing/latest/domain/SmartphoneStudyProtocol-class.html).
2. Deploy this protocol to the [`SmartPhoneClientManager`](https://pub.dev/documentation/carp_mobile_sensing/latest/runtime/SmartPhoneClientManager-class.html).
3. Use the generated data (called `measurements`) locally in the app or specify how and where to store or upload it using a [`DataEndPoint`](https://pub.dev/documentation/carp_mobile_sensing/latest/domain/DataEndPoint-class.html).
4. Control the execution of the study, like calling [`start`](https://pub.dev/documentation/carp_mobile_sensing/latest/runtime/SmartPhoneClientManager/start.html).

### Defining a `SmartphoneStudyProtocol`

In CAMS, a sensing protocol is configured in a [`SmartphoneStudyProtocol`](https://pub.dev/documentation/carp_mobile_sensing/latest/domain/SmartphoneStudyProtocol-class.html).
Below is a simple example of how to set up a protocol that samples step counts, ambient light, screen events, and battery events.

```dart
final phone = Smartphone();
final protocol = SmartphoneStudyProtocol(
  ownerId: 'abc@dtu.dk',
  name: 'Tracking steps, light, screen, and battery',
  dataEndPoint: SQLiteDataEndPoint(),
)
  ..addPrimaryDevice(phone)
  ..addTaskControl(
    DelayedTrigger(delay: const Duration(seconds: 10)),
    BackgroundTask(measures: [
      Measure(type: SensorSamplingPackage.STEP_COUNT),
      Measure(type: SensorSamplingPackage.AMBIENT_LIGHT),
      Measure(type: DeviceSamplingPackage.SCREEN_EVENT),
      Measure(type: DeviceSamplingPackage.BATTERY_STATE),
    ]),
    phone,
    Control.Start,
  );
```

The above example defines a simple [`SmartphoneStudyProtocol`](https://pub.dev/documentation/carp_mobile_sensing/latest/domain/SmartphoneStudyProtocol-class.html) which will use a [`Smartphone`](https://pub.dev/documentation/carp_core/latest/carp_core_common/Smartphone-class.html) as a primary device for data collection and store data in a SQLite database locally on the phone using a [`SQLiteDataEndPoint`](https://pub.dev/documentation/carp_mobile_sensing/latest/domain/SQLiteDataEndPoint-class.html).
Sampling is configured by adding a [`TaskControl`](https://pub.dev/documentation/carp_core/latest/carp_core_common/TaskControl-class.html) to the protocol using an [`DelayedTrigger`](https://pub.dev/documentation/carp_mobile_sensing/latest/domain/DelayedTrigger-class.html) which triggers a [`BackgroundTask`](https://pub.dev/documentation/carp_core/latest/carp_core_common/BackgroundTask-class.html) containing four different [`Measure`](https://pub.dev/documentation/carp_core/latest/carp_core_common/Measure-class.html)s.
When this task control is triggered (after a delay of 10 seconds), the sampling will start.

Sampling can be configured in very sophisticated ways, by specifying different types of devices, task controls, triggers, tasks, measures, and sampling configurations.
See the CAMS [wiki][wiki] for an overview and more details.

### Deploying and Running a Study on a `SmartPhoneClientManager`

In CAMS, we talk about a study protocol being 'deployed' on a primary device, like a phone. CAMS has a fairly [sophisticated software architecture](https://github.com/cph-cachet/carp.sensing-flutter/wiki/1.-Software-Architecture) for doing this.
However, if we just want to define and deploy a study locally on the phone, this can be done using the [`SmartPhoneClientManager`](https://pub.dev/documentation/carp_mobile_sensing/latest/runtime/SmartPhoneClientManager-class.html) singleton.

```dart
// Create and configure a client manager for this phone.
await SmartPhoneClientManager().configure();

// Create a study based on the protocol.
SmartPhoneClientManager().addStudyProtocol(protocol);

/// Start sampling.
SmartPhoneClientManager().start();
```

In this example, the client manager is configured, the protocol is added, and sampling is started. This can actually be done in one line of code, like this:

```dart
SmartPhoneClientManager().configure().then((_) => SmartPhoneClientManager()
    .addStudyProtocol(protocol)
    .then((_) => SmartPhoneClientManager().start()));
```

This will start the sampling, as specified in the protocol, and data is stored in the database.

### Using the generated data

The generated data can be accessed and used in the app. Access to data is done by listening on the [`measurements`](https://pub.dev/documentation/carp_mobile_sensing/latest/runtime/SmartPhoneClientManager/measurements.html) stream from the client manager, like this:

```dart
// Listening on the data stream and print them as json.
SmartPhoneClientManager()
    .measurements
    .listen((measurement) => print(toJsonString(measurement)));
```

Note that `measurements` is a Dart [Stream](https://api.dart.dev/stable/3.0.7/dart-async/Stream-class.html) and you can hence apply all the usual stream operations to the collected measurements, including sorting, mapping, reducing, and transforming measurements.

### Controlling the sampling of data

The execution of sensing can be controlled on runtime by starting, stopping, and disposing sampling.
For example, calling `SmartPhoneClientManager().stop()` would stop the study running on the client. Calling `start()` would (re)start it again.

Calling `SmartPhoneClientManager().dispose()` would dispose of the client manager. Once dispose is called, you cannot call `start` or `stop` anymore. This methods is typically used in the Flutter `dispose()` method.

## Extending CAMS

CAMS is designed to be extended in many ways, including [adding new sampling capabilities](https://github.com/cph-cachet/carp.sensing-flutter/wiki/5.-Extending-CARP-Mobile-Sensing#adding-new-sampling-capabilities) by implementing a Sampling Package, [adding a new data management and backend support](https://github.com/cph-cachet/carp.sensing-flutter/wiki/5.-Extending-CARP-Mobile-Sensing#adding-a-new-data-manager) by creating a Data Manager, and [creating data and privacy transformer schemas](https://github.com/cph-cachet/carp.sensing-flutter/wiki/5.-Extending-CARP-Mobile-Sensing#adding-data-and-privacy-transformers) that can transform CARP data to other formats, including privacy protecting them, by implementing a [Transformer Schema](https://pub.dev/documentation/carp_mobile_sensing/latest/domain/DataTransformerSchema-class.html).

For example, you can write your own `DataEndPoint` definitions and a corresponding [`DataManager`](https://pub.dev/documentation/carp_mobile_sensing/latest/runtime/DataManager-class.html) class for uploading data to your own data endpoint. See the wiki on how to [add a new data manager](https://github.com/cph-cachet/carp.sensing-flutter/wiki/5.-Extending-CARP-Mobile-Sensing#adding-a-new-data-manager).

Please see the wiki on how to [extend](https://github.com/cph-cachet/carp.sensing-flutter/wiki/5.-Extending-CARP-Mobile-Sensing) CAMS.

## Features and bugs

Please read about existing issues and file new feature requests and bug reports at the [issue tracker][tracker].

## License

This software is copyright (c) [Copenhagen Center for Health Technology (CACHET)](https://www.cachet.dk/) at the [Technical University of Denmark (DTU)](https://www.dtu.dk).
This software is available 'as-is' under a [MIT license](https://github.com/cph-cachet/carp.sensing-flutter/blob/master/LICENSE).

<!-- LINKS  -->

[tracker]: https://github.com/cph-cachet/carp.sensing-flutter/issues
[wiki]: https://github.com/cph-cachet/carp.sensing-flutter/wiki
