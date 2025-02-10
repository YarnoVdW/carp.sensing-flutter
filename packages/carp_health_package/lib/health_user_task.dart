part of 'health_package.dart';

/// A non-UI user task that collects health data in the background.
///
/// When started, it will ask for permission to access the health data listed
/// in the [HealthAppTask].
class HealthUserTask extends UserTask {
  static const String HEALTH_ASSESSMENT_TYPE = 'health';

  // Health health = Health();

  /// The [HealthAppTask] which specifies which health data to collect.
  HealthAppTask get healthAppTask => super.task as HealthAppTask;

  HealthUserTask(super.executor);

  @override
  void onStart() {
    // first initialize the background task executor
    super.onStart();

    // then check for permission to access health data
    try {
      var healthProbe = backgroundTaskExecutor.probes
          .firstWhere((probe) => probe is HealthProbe) as HealthProbe;

      healthProbe.hasPermissions().then((granted) {
        if (granted) {
          debug(
              '$runtimeType - Already has permissions to access health data.');
          _startProbeAndStopAgain();
        } else {
          healthProbe.requestPermissions().then((granted) {
            if (granted) {
              debug('$runtimeType - Got permissions to access health data.');
              _startProbeAndStopAgain();
            } else {
              warning(
                  '$runtimeType - Could not get permissions to access health data.');
              return;
            }
          });
        }
      });
    } catch (error) {
      // if the health probe is not found in the list of probes, we cannot
      // access health data.
      warning(
          '$runtimeType - No health probe found in list of probes. Does the '
          'study protocol include any health data types?');
    }
  }

  void _startProbeAndStopAgain() {
    backgroundTaskExecutor.start();
    Timer(const Duration(seconds: 10), () => onDone());
  }

  @override
  void onDone({dequeue = false, Data? result}) {
    super.onDone(dequeue: dequeue, result: result);
    backgroundTaskExecutor.stop();
  }
}

class HealthUserTaskFactory implements UserTaskFactory {
  @override
  List<String> types = [
    HealthUserTask.HEALTH_ASSESSMENT_TYPE,
  ];

  // always create a [HealthUserTask]
  @override
  UserTask create(AppTaskExecutor executor) => HealthUserTask(executor);
}
