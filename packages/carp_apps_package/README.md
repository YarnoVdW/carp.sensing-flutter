# CARP Apps Sampling Package

[![pub package](https://img.shields.io/pub/v/carp_apps_package.svg)](https://pub.dartlang.org/packages/carp_context_package)
[![pub points](https://img.shields.io/pub/points/carp_apps_package?color=2E8B57&label=pub%20points)](https://pub.dev/packages/carp_apps_package/score)
[![github stars](https://img.shields.io/github/stars/cph-cachet/carp.sensing-flutter.svg?style=flat&logo=github&colorB=deeppink&label=stars)](https://github.com/cph-cachet/carp.sensing-flutter)
[![MIT License](https://img.shields.io/badge/license-MIT-purple.svg)](https://opensource.org/licenses/MIT)
[![arXiv](https://img.shields.io/badge/arXiv-2006.11904-green.svg)](https://arxiv.org/abs/2006.11904)

This library contains a sampling package for app-related sampling to work with
the [`carp_mobile_sensing`](https://pub.dartlang.org/packages/carp_mobile_sensing) framework.
This packages supports sampling of the following [`Measure`](https://github.com/cph-cachet/carp.sensing-flutter/wiki/A.-Measure-Types) types:

* `dk.cachet.carp.apps` - a list of installed apps on the phone.
* `dk.cachet.carp.app_usage` - a log of app usage activity.

These measures are only available on Android.

See the [wiki](https://github.com/cph-cachet/carp.sensing-flutter/wiki) for further documentation, particularly on available [measure types](https://github.com/cph-cachet/carp.sensing-flutter/wiki/A.-Measure-Types).
See the [CARP Mobile Sensing App](https://github.com/cph-cachet/carp.sensing-flutter/tree/master/apps/carp_mobile_sensing_app) for an example of how to build a mobile sensing app in Flutter.

For Flutter plugins for other CARP products, see [CARP Mobile Sensing in Flutter](https://github.com/cph-cachet/carp.sensing-flutter).

If you're interested in writing you own sampling packages for CARP, see the description on
how to [extend](https://github.com/cph-cachet/carp.sensing-flutter/wiki/5.-Extending-CARP-Mobile-Sensing) CARP on the wiki.

## Installing

To use this package, add the following to you `pubspc.yaml` file. Note that
this package only works together with `carp_mobile_sensing`.

`````dart
dependencies:
  flutter:
    sdk: flutter
  carp_core: ^latest
  carp_mobile_sensing: ^latest
  carp_apps_package: ^latest
  ...
`````

### Android Integration

Add the following to your app's `AndroidManifest.xml` file located in `android/app/src/main` such that it contains the following permission request:

````xml
<manifest xmlns:android="http://schemas.android.com/apk/res/android"
    package="<YOUR_PACKAGE_NAME>"
    xmlns:tools="http://schemas.android.com/tools">

   <uses-permission android:name="android.permission.PACKAGE_USAGE_STATS" tools:ignore="ProtectedPermissions"/>
   <uses-permission android:name="android.permission.QUERY_ALL_PACKAGES" />
   ...
</manifest>
````

Starting with Android 11, Android applications targeting API level 30, wanting to list "external" applications have to declare a new "normal" permission in their `AndroidManifest.xml` file called [`QUERY_ALL_PACKAGES`](https://developer.android.com/reference/kotlin/android/Manifest.permission#query_all_packages).

Starting from [May 5 2021](https://support.google.com/googleplay/android-developer/answer/10158779), Google will mark a breaking change on how applications requesting [`QUERY_ALL_PACKAGES`](https://developer.android.com/reference/kotlin/android/Manifest.permission#query_all_packages) are accepted in the Google Play. [Quoting from the doc](https://support.google.com/googleplay/android-developer/answer/10158779):

> Permitted use involves apps that must discover any and all installed apps on the device, for awareness or interoperability purposes may have eligibility for the permission. Permitted use includes; device search, antivirus apps, file managers, and browsers.
>
> Apps granted access to this permission must comply with the User Data policies, including the Prominent Disclosure and Consent requirements, and may not extend its use to undisclosed or invalid purposes.

### iOS Integration

Not supported.

## Using it

To use this package, import it into your app together with the
[`carp_mobile_sensing`](https://pub.dartlang.org/packages/carp_mobile_sensing) package:

```dart
import 'package:carp_core/carp_core.dart';
import 'package:carp_mobile_sensing/carp_mobile_sensing.dart';
import 'package:carp_apps_package/apps.dart';
```

Before creating a study and running it, register this package in the
[SamplingPackageRegistry](https://pub.dartlang.org/documentation/carp_mobile_sensing/latest/runtime/SamplingPackageRegistry.html).

```dart
SamplingPackageRegistry().register(AppsSamplingPackage());
```

Collection of `APPS` and `APP_USAGE` measures can be added to a study protocol like this.

```dart
// Define which devices are used for data collection
// In this case, its only this smartphone
Smartphone phone = Smartphone();
protocol.addPrimaryDevice(phone);

// Add an automatic task that collects the list of installed apps
// and a log of app usage activity
protocol.addTaskControl(
    ImmediateTrigger(),
    BackgroundTask(measures: [
      Measure(type: AppsSamplingPackage.APPS),
      Measure(type: AppsSamplingPackage.APP_USAGE),
    ]),
    phone);
```
