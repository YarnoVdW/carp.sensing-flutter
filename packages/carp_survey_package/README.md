# CARP Survey Sampling Package

[![pub package](https://img.shields.io/pub/v/carp_connectivity_package.svg)](https://pub.dartlang.org/packages/carp_connectivity_package)
[![pub points](https://img.shields.io/pub/points/carp_connectivity_package?color=2E8B57&label=pub%20points)](https://pub.dev/packages/carp_connectivity_package/score)
[![github stars](https://img.shields.io/github/stars/cph-cachet/carp.sensing-flutter.svg?style=flat&logo=github&colorB=deeppink&label=stars)](https://github.com/cph-cachet/carp.sensing-flutter)
[![MIT License](https://img.shields.io/badge/license-MIT-purple.svg)](https://opensource.org/licenses/MIT)
[![arXiv](https://img.shields.io/badge/arXiv-2006.11904-green.svg)](https://arxiv.org/abs/2006.11904)

This library contains a sampling package for collection of user-generated data to work with the [`carp_mobile_sensing`](https://pub.dartlang.org/packages/carp_mobile_sensing) framework. Data is collected via [surveys](https://carp.cachet.dk/creating-a-survey/) and [cognitive tests](https://carp.cachet.dk/creating-cognitive-tests/).
For this, this library uses the [CARP Research Package](https://carp.cachet.dk/research-package/) and the [CARP Cognition Package](https://carp.cachet.dk/cognition-package/).
This package supports the creation of a `RPAppTask` which can be added to a CAMS study protocol.

Read more on the [Research Package API](https://carp.cachet.dk/research-package-api/) and how to [create a survey](https://carp.cachet.dk/creating-a-survey/) and how to [create a cognitive test](https://carp.cachet.dk/creating-cognitive-tests/) on the CARP website. For a demo of how to use this package and the `RPAppTask` in an app, see the [PulmonaryMonitor](https://github.com/cph-cachet/pulmonary_monitor_app) app. To read more about using the `AppTask`, see the [CAMS wiki](https://github.com/cph-cachet/carp.sensing-flutter/wiki/4.-The-AppTask-Model).

## Installing

To use this package, add the following to you `pubspc.yaml` file. Note that
this package only works together with `carp_mobile_sensing`.

`````dart
dependencies:
  carp_mobile_sensing: ^latest
  carp_survey_package: ^latest
  ...
`````

## Using it

To use this package, import it into your app together with the
[`carp_mobile_sensing`](https://pub.dartlang.org/packages/carp_mobile_sensing) package:

`````dart
import 'package:carp_core/carp_core.dart';
import 'package:carp_mobile_sensing/carp_mobile_sensing.dart';
import 'package:carp_survey_package/survey.dart';
`````

Before creating a study and running it, register this package in the
[SamplingPackageRegistry](https://pub.dartlang.org/documentation/carp_mobile_sensing/latest/runtime/SamplingPackageRegistry.html).

`````dart
SamplingPackageRegistry().register(SurveySamplingPackage());
`````

Once this is in place, a survey can be added as a `RPAppTask` to a CAMS protocol like this:

```dart
// Add a task control to the protocol that triggers every day at 13:00,
// issuing a WHO-5 survey while also collecting device and
// ambient light information when survey is initiated by the user.
protocol.addTaskControl(
    RecurrentScheduledTrigger(
      type: RecurrentType.daily,
      time: TimeOfDay(hour: 13),
    ),
    RPAppTask(
        type: SurveyUserTask.SURVEY_TYPE,
        name: 'WHO-5 Survey',
        rpTask: who5Task,
        measures: [
          Measure(type: DeviceSamplingPackage.DEVICE_INFORMATION),
          Measure(type: SensorSamplingPackage.AMBIENT_LIGHT),
        ]),
    phone);
````

A set of cognitive test can be added like this:

```dart
// Add a Parkinson's assessment consisting of;
//  * an instruction step
//  * a timer step
//  * a Flanker and Tapping activity (from cognition package).
//
// Accelerometer and gyroscope data is collected while the user is performing
// the task in oder to assess tremor.
protocol.addTaskControl(
  PeriodicTrigger(period: const Duration(hours: 2)),
  RPAppTask(
      type: SurveyUserTask.COGNITIVE_ASSESSMENT_TYPE,
      title: "Parkinson's' Assessment",
      description: "A simple task assessing motor and cognitive functioning.",
      minutesToComplete: 3,
      rpTask: RPOrderedTask(
        identifier: "parkinsons_assessment",
        steps: [
          RPInstructionStep(
              identifier: 'parkinsons_instruction',
              title: "Parkinsons' Disease Assessment",
              text:
                  "In the following pages, you will be asked to solve two simple test which will help assess "
                  "your symptoms on a daily basis. Each test has an instruction page, which you should read "
                  "carefully before starting the test.\n\n"
                  "Please sit down comfortably and hold the phone in one hand while performing the test "
                  "with the other."),
          RPTimerStep(
            identifier: 'timer_1',
            timeout: const Duration(seconds: 6),
            title:
                "Please stand up and hold the phone in one hand and lift it in a straight arm "
                "until you hear the sound.",
            playSound: true,
          ),
          RPFlankerActivity(
            identifier: 'flanker_1',
            lengthOfTest: 30,
            numberOfCards: 10,
          ),
          RPTappingActivity(
            identifier: 'tapping_1',
            lengthOfTest: 10,
          )
        ],
      ),
      measures: [
        Measure(type: SensorSamplingPackage.ACCELERATION),
        Measure(type: SensorSamplingPackage.ROTATION),
      ]),
  phone);
  ```
