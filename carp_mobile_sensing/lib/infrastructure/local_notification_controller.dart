/*
 * Copyright 2022 Copenhagen Center for Health Technology (CACHET) at the
 * Technical University of Denmark (DTU).
 * Use of this source code is governed by a MIT-style license that can be
 * found in the LICENSE file.
 */

part of '../infrastructure.dart';

/// A [NotificationController] based on the [flutter_local_notifications](https://pub.dev/packages/flutter_local_notifications)
/// Flutter plugin.
///
/// On iOS, remember to edit the AppDelegate.swift file.
/// See https://pub.dev/packages/flutter_local_notifications#general-setup
///
/// On Android, add an `app_icon.png` square png picture in the
/// `<<application_name>>/android/app/src/main/res/drawable/` folder.
class FlutterLocalNotificationController implements NotificationController {
  static final FlutterLocalNotificationController _instance =
      FlutterLocalNotificationController._();
  FlutterLocalNotificationController._() : super();

  final Random _random = Random();

  /// The singleton [NotificationController].
  factory FlutterLocalNotificationController() => _instance;

  /// Initialize and set up the notification controller.
  @override
  Future<void> initialize() async {
    tz.initializeTimeZones();

    List<Permission> permissions =
        List.from([Permission.notification, Permission.scheduleExactAlarm]);
    var status = await permissions.request();
    debug('$runtimeType - permissions: $status');

    await FlutterLocalNotificationsPlugin().initialize(
      const InitializationSettings(
        android: AndroidInitializationSettings('app_icon'),
        iOS: DarwinInitializationSettings(),
      ),
      onDidReceiveBackgroundNotificationResponse:
          onDidReceiveNotificationResponse,
      onDidReceiveNotificationResponse: onDidReceiveNotificationResponse,
    );

    info('$runtimeType initialized.');

    debug('$runtimeType - Pending Notifications:');
    for (var notification in (await FlutterLocalNotificationsPlugin()
        .pendingNotificationRequests())) {
      debug('  - ${notification.title}');
    }
  }

  final NotificationDetails _platformChannelSpecifics =
      const NotificationDetails(
    android: AndroidNotificationDetails(
      NotificationController.CHANNEL_ID,
      NotificationController.CHANNEL_NAME,
      channelDescription: NotificationController.CHANNEL_DESCRIPTION,
      importance: Importance.max,
      priority: Priority.max,
      ongoing: true,
    ),
    iOS: DarwinNotificationDetails(),
  );

  @override
  Future<int> createNotification({
    int? id,
    required String title,
    String? body,
  }) async {
    id ??= _random.nextInt(1000);
    await FlutterLocalNotificationsPlugin().show(
      id,
      title,
      body,
      _platformChannelSpecifics,
    );
    return id;
  }

  @override
  Future<int> scheduleNotification(
      {int? id,
      required String title,
      String? body,
      required DateTime schedule}) async {
    id ??= _random.nextInt(1000);
    final time =
        tz.TZDateTime.from(schedule, tz.getLocation(Settings().timezone));

    await FlutterLocalNotificationsPlugin().zonedSchedule(
      id,
      title,
      body,
      time,
      _platformChannelSpecifics,
      androidScheduleMode: AndroidScheduleMode.exactAllowWhileIdle,
      uiLocalNotificationDateInterpretation:
          UILocalNotificationDateInterpretation.absoluteTime,
    );

    return id;
  }

  @override
  Future<int> scheduleRecurrentNotifications(
      {int? id,
      required String title,
      String? body,
      required RecurrentScheduledTrigger schedule}) async {
    id ??= _random.nextInt(1000);
    final time = tz.TZDateTime.from(
        schedule.firstOccurrence, tz.getLocation(Settings().timezone));

    DateTimeComponents recurrence = switch (schedule.type) {
      RecurrentType.daily => DateTimeComponents.time,
      RecurrentType.weekly => DateTimeComponents.dayOfWeekAndTime,
      RecurrentType.monthly => DateTimeComponents.dayOfMonthAndTime,
    };

    await FlutterLocalNotificationsPlugin().zonedSchedule(
      id,
      title,
      body,
      time,
      _platformChannelSpecifics,
      androidScheduleMode: AndroidScheduleMode.exactAllowWhileIdle,
      uiLocalNotificationDateInterpretation:
          UILocalNotificationDateInterpretation.absoluteTime,
      matchDateTimeComponents: recurrence,
    );

    return id;
  }

  @override
  Future<void> cancelNotification(int id) async =>
      await FlutterLocalNotificationsPlugin().cancel(id);

  @override
  Future<void> createTaskNotification(UserTask task) async {
    if (task.notification) {
      await FlutterLocalNotificationsPlugin().show(
        task.id.hashCode,
        task.title,
        task.description,
        _platformChannelSpecifics,
        payload: task.id,
      );
      info('$runtimeType - Notification created for $task');
    }
  }

  @override
  Future<void> scheduleTaskNotification(UserTask task) async {
    // early out if not to be scheduled
    if (!task.notification) return;

    if (task.triggerTime.isAfter(DateTime.now())) {
      final time = tz.TZDateTime.from(
          task.triggerTime, tz.getLocation(Settings().timezone));

      await FlutterLocalNotificationsPlugin().zonedSchedule(
        task.id.hashCode,
        task.title,
        task.description,
        time,
        _platformChannelSpecifics,
        androidScheduleMode: AndroidScheduleMode.exactAllowWhileIdle,
        uiLocalNotificationDateInterpretation:
            UILocalNotificationDateInterpretation.absoluteTime,
        payload: task.id,
      );
      task.hasNotificationBeenCreated = true;
      debug('$runtimeType - Notification scheduled for $task at $time');
    } else {
      warning('$runtimeType - Can only schedule a notification in the future. '
          'task trigger time: ${task.triggerTime}.');
    }
  }

  @override
  Future<int> get pendingNotificationRequestsCount async =>
      (await FlutterLocalNotificationsPlugin().pendingNotificationRequests())
          .length;

  @override
  Future<void> cancelTaskNotification(UserTask task) async {
    if (task.notification) {
      await FlutterLocalNotificationsPlugin().cancel(task.id.hashCode);
      info('$runtimeType - Notification canceled for $task');
    }
  }
}

/// Callback method called when a notification is clicked in the operating system.
@pragma('vm:entry-point')
void onDidReceiveNotificationResponse(NotificationResponse response) {
  String? payload = response.payload;
  debug('NotificationController - callback on notification, payload: $payload');

  if (payload != null) {
    AppTaskController().onNotification(payload);
  } else {
    warning(
        "NotificationController - Error in callback from notification - payload is '$payload'");
  }
}
