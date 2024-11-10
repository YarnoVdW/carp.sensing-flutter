/*
 * Copyright 2020 Copenhagen Center for Health Technology (CACHET) at the
 * Technical University of Denmark (DTU).
 * Use of this source code is governed by a MIT-style license that can be
 * found in the LICENSE file.
 */

part of '../../runtime.dart';

/// The [TaskExecutor] is responsible for executing a [TaskConfiguration].
/// For each measure in the task, it looks up an appropriate [Probe] to
/// collect data.
///
/// Note that a [TaskExecutor] in itself is a [Executor].
/// This - amongst other things - imply that you can listen
/// to [Executor.measurements] from a task executor.
abstract class TaskExecutor<TConfig extends TaskConfiguration>
    extends AggregateExecutor<TConfig> {
  final StreamGroup<ExecutorState> _statesGroup = StreamGroup.broadcast();

  /// The [TaskConfiguration] for this task executor.
  TConfig get task => configuration!;

  /// Returns a list of the probes in this task executor.
  List<Probe> get probes =>
      executors.map((executor) => executor as Probe).toList();

  /// The combines state event from all [probes] in this task executor.
  Stream<ExecutorState> get states => _statesGroup.stream;

  @override
  bool onInitialize() {
    if (task.measures != null) {
      for (Measure measure in task.measures!) {
        // create a new probe for each measure - this ensures that we can have
        // multiple measures of the same type, each using its own probe instance
        Probe? probe = SamplingPackageRegistry().create(measure.type);
        if (probe != null) {
          addExecutor(probe);
          _statesGroup.add(probe.stateEvents);

          probe.initialize(measure, deployment!);
        } else {
          warning(
              "A probe for measure type '${measure.type}' could not be created. "
              'This may be because this probe is not available on the operating system '
              'of this phone (primary device) or on the connected device. '
              'Or it may be because the sampling package containing this probe has not '
              'been registered in the SamplingPackageRegistry.');
        }
      }
    }
    return true;
  }
}

/// Executes a [BackgroundTask].
class BackgroundTaskExecutor extends TaskExecutor<BackgroundTask> {
  StreamSubscription<ExecutorState>? _subscription;

  /// Are all [probes] in a stopped state?
  bool get haveAllProbesStopped =>
      !probes.any((probe) => probe.state != ExecutorState.stopped);

  /// Connect all connectable devices used by the [probes] in this
  /// background task executor.
  Future<void> connectAllConnectableDevices() async {
    debug(
        '$runtimeType - Trying to connect to all connectable devices for this background executor.');

    probes
        .where((probe) => !probe.deviceManager.isConnecting)
        .forEach((probe) async => await probe.deviceManager.connect());
  }

  @override
  Future<bool> onStart() async {
    // Early out if no probes.
    if (probes.isEmpty) return true;

    // Early out if already running (this is a background task)
    if (state == ExecutorState.started) {
      warning(
          'Trying to start $this but it is already started. Ignoring this.');
      return false;
    }

    // Listen to stop this background executor when all of its underlying
    // probes have stopped - Issue #384
    _subscription =
        states.where((event) => event == ExecutorState.stopped).listen((_) {
      if (haveAllProbesStopped) {
        debug(
            '$runtimeType - all probes have stopped - stopping this $this too.');
        stop();
      }
    });

    // Check if the devices for this task is connected.
    await connectAllConnectableDevices();

    if (configuration?.duration != null) {
      // If the task has a duration (optional), stop it again after this duration has passed.
      Timer(Duration(seconds: configuration!.duration!.inSeconds.truncate()),
          () => stop());
    }

    // Now - finally - we can start the probes.
    return await super.onStart();
  }

  @override
  Future<bool> onRestart() async {
    _subscription?.cancel();
    return super.onRestart();
  }

  @override
  Future<bool> onStop() async {
    _subscription?.cancel();
    return super.onStop();
  }
}

/// Executes a [FunctionTask].
class FunctionTaskExecutor extends TaskExecutor<FunctionTask> {
  @override
  Future<bool> onStart() async {
    if (configuration?.function != null) {
      Function.apply(configuration!.function!, []);
    }
    return true;
  }
}

/// Executes an [AppTask].
///
/// This executor works closely with the singleton [AppTaskController].
/// Whenever an [AppTaskExecutor] is started (e.g. in a [PeriodicTrigger]),
/// this executor is wrapped in a [UserTask] and put on a queue in
/// the [AppTaskController].
///
/// Later, the app (user) can start, cancel, or finalize a [UserTask]
/// by calling the `onStart()`, `onCancel()`, and `onDone()` methods,
/// respectively.
///
/// Special-purpose [UserTask]s can be created by an [UserTaskFactory]
/// and such factories can be registered in the [AppTaskController]
/// using the `registerUserTaskFactory` method.
class AppTaskExecutor<TConfig extends AppTask> extends TaskExecutor<TConfig> {
  @override
  bool onInitialize() => true;

  @override
  Future<bool> onStart() async {
    // when an app task is started, create a new UserTask by adding it to the queue
    UserTask? userTask = await AppTaskController().enqueue(this);

    // automatically stop this executor again to be reused later
    // issue => https://github.com/cph-cachet/carp.sensing-flutter/issues/429
    Future.delayed(const Duration(seconds: 5), () => stop());

    return userTask != null;
  }

  // does nothing when stopping an app task
  @override
  Future<bool> onStop() async => true;
}
