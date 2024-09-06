import 'package:carp_serializable/carp_serializable.dart';
import 'package:carp_core/carp_core.dart';
import 'package:carp_mobile_sensing/carp_mobile_sensing.dart';
import 'package:carp_webservices/carp_auth/carp_auth.dart';
import 'package:carp_webservices/carp_services/carp_services.dart';
import 'package:flutter/cupertino.dart';
import 'package:test/test.dart';
import 'package:shared_preferences/shared_preferences.dart';

import '_credentials.dart';
import '_carp_properties.dart';

/// This test suite tests how to set and get expected participation data.
///
/// The main focus is to test
///  - creating a protocol with expected participation data
///  - setting & getting participation data for different participant roles
///
/// A special protocol for a 3-person family is used for testing expected
/// participation data for each family member.
void main() {
  const father = 'Father';
  const mother = 'Mother';
  const child = 'Child';

  CarpUser? user;
  String? ownerId;

  Settings().debugLevel = DebugLevel.debug;
  SharedPreferences.setMockInitialValues({});
  WidgetsFlutterBinding.ensureInitialized();
  CarpMobileSensing.ensureInitialized();

  /// Configure CARP and authenticate.
  setUpAll(() async {
    await CarpAuthService().configure(CarpProperties().authProperties);

    CarpApp app = CarpApp(
      name: "CAWS @ $cawsUri",
      uri: Uri(
        scheme: 'https',
        host: cawsUri,
      ),
      studyDeploymentId: familyDeploymentId,
      studyId: familyStudyId,
    );

    CarpService().configure(app);

    user = await CarpAuthService().authenticateWithUsernamePassword(
      username: fatherUsername,
      password: fatherPassword,
    );
    CarpDeploymentService().configureFrom(CarpService());
    CarpParticipationService().configureFrom(CarpService());
  });

  tearDownAll(() {});

  group("Base Services", () {
    test('- authentication', () async {
      debugPrint('CarpService : ${CarpService().app}');
      debugPrint(" - signed in as: $user");
    });

    test(
      '- get invitations for this user',
      () async {
        List<ActiveParticipationInvitation> invitations =
            await CarpParticipationService()
                .getActiveParticipationInvitations();

        expect(invitations, isNotNull);
        debugPrint(toJsonString(invitations));
      },
      skip: false,
    );
  });

  group("Define Protocol", () {
    test(
      '- define',
      () async {
        ownerId = CarpAuthService().currentUser.id;

        // a study protocol testing misc input data types for a family
        StudyProtocol protocol = StudyProtocol(
            ownerId: ownerId!,
            name: 'Input Data Types',
            description: 'Collect mics. input from study participants.');

        // add participant roles
        protocol
          ..addParticipantRole(ParticipantRole(father))
          ..addParticipantRole(ParticipantRole(mother))
          ..addParticipantRole(ParticipantRole(child));

        // define and assign the primary devices for each family member
        final fatherPhone = Smartphone(roleName: "$father's Phone");
        final motherPhone = Smartphone(roleName: "$mother's Phone");
        final childPhone = Smartphone(roleName: "$child's Phone");
        protocol
          ..addPrimaryDevice(fatherPhone)
          ..addPrimaryDevice(motherPhone)
          ..addPrimaryDevice(childPhone);

        protocol
          ..changeDeviceAssignment(fatherPhone, AssignedTo(roleNames: {father}))
          ..changeDeviceAssignment(motherPhone, AssignedTo(roleNames: {mother}))
          ..changeDeviceAssignment(childPhone, AssignedTo(roleNames: {child}));

        // add expected participant data which can be set by ALL participants
        protocol.addExpectedParticipantData(ExpectedParticipantData(
            attribute: ParticipantAttribute(inputDataType: AddressInput.type)));

        // add expected participant data for specific participants
        protocol
          ..addExpectedParticipantData(ExpectedParticipantData(
              attribute:
                  ParticipantAttribute(inputDataType: FullNameInput.type),
              assignedTo: AssignedTo(roleNames: {father, mother, child})))
          ..addExpectedParticipantData(ExpectedParticipantData(
              attribute: ParticipantAttribute(inputDataType: SexInput.type),
              assignedTo: AssignedTo(roleNames: {father, mother, child})))
          ..addExpectedParticipantData(ExpectedParticipantData(
              attribute: ParticipantAttribute(
                  inputDataType: InformedConsentInput.type),
              assignedTo: AssignedTo(roleNames: {father, mother})));

        // add measures (not really used in this test)
        protocol
          ..addTaskControl(
              ImmediateTrigger(),
              BackgroundTask(measures: [
                Measure(type: SensorSamplingPackage.STEP_COUNT),
                Measure(type: SensorSamplingPackage.AMBIENT_LIGHT),
              ]),
              fatherPhone)
          ..addTaskControl(
              ImmediateTrigger(),
              BackgroundTask(measures: [
                Measure(type: DeviceSamplingPackage.SCREEN_EVENT),
                Measure(type: DeviceSamplingPackage.FREE_MEMORY),
              ]),
              motherPhone)
          ..addTaskControl(
              ImmediateTrigger(),
              BackgroundTask(measures: [
                Measure(type: DeviceSamplingPackage.SCREEN_EVENT),
                Measure(type: DeviceSamplingPackage.BATTERY_STATE),
              ]),
              childPhone);
        print(toJsonString(protocol));
      },
    );
  });
  group("Deployment", () {
    test('- get deployment status', () async {
      final status = await CarpDeploymentService().deployment().getStatus();
      debugPrint(toJsonString(status));
      expect(status.studyDeploymentId, familyDeploymentId);
    });

    test('- register device', () async {
      final reference = CarpDeploymentService().deployment();
      var status = await reference.getStatus();
      debugPrint('$status');

      expect(status.primaryDeviceStatus!.device, isNotNull);
      debugPrint('${status.primaryDeviceStatus!.device}');
      var newStatus = await reference.registerPrimaryDevice();
      debugPrint('$newStatus');
      expect(newStatus.studyDeploymentId, familyDeploymentId);
    });

    test('- get primary device deployment', () async {
      final reference = CarpDeploymentService().deployment();
      final status = await reference.getStatus();
      debugPrint('$status');
      expect(status.primaryDeviceStatus!.device, isNotNull);
      debugPrint('${status.primaryDeviceStatus!.device}');

      PrimaryDeviceDeployment deployment = await reference.get();
      debugPrint(toJsonString(deployment));
      expect(deployment.registration.deviceId, isNotNull);
    });
  });

  group("Participant Data", () {
    test(
      '- get all',
      () async {
        ParticipationReference participation =
            CarpParticipationService().participation();

        ParticipantData data = await participation.getParticipantData();
        debugPrint(toJsonString(data));

        // data.roles.forEach(
        //     (data) => debugPrint('${data.roleName} : ${data.data.keys}'));
      },
    );

    test(
      '- set common data (Address)',
      () async {
        final participation = CarpParticipationService().participation();

        ParticipantData data = await participation.setParticipantData(
          {
            AddressInput.type: AddressInput(
              address1: 'DTU HealthTech',
              address2: 'Technical University of Denmark',
              street: 'Ørsteds Plads',
              city: 'Kgs. Lyngby',
              postalCode: 'DK-2800',
              country: 'Denmark',
            )
          },
        );
        debugPrint(toJsonString(data));

        expect(data.common[AddressInput.type], isA<AddressInput>());
        expect((data.common[AddressInput.type] as AddressInput).country,
            'Denmark');
      },
    );

    test(
      '- set role-specific data (Sex of Father)',
      () async {
        final participation = CarpParticipationService().participation();

        final data = await participation.setParticipantData(
          {SexInput.type: SexInput(value: Sex.Male)},
          father,
        );
        debugPrint(toJsonString(data));

        final sex = data.roles
            .firstWhere((role) => role.roleName == father)
            .data[SexInput.type];

        expect(sex, isA<SexInput>());
        expect((sex as SexInput).value, Sex.Male);
      },
    );

    test(
      '- set role-specific data (Sex of Mother)',
      () async {
        final participation = CarpParticipationService().participation();

        final data = await participation.setParticipantData(
          {SexInput.type: SexInput(value: Sex.Female)},
          mother,
        );
        debugPrint(toJsonString(data));

        final sex = data.roles
            .firstWhere((role) => role.roleName == mother)
            .data[SexInput.type];

        expect(sex, isA<SexInput>());
        expect((sex as SexInput).value, Sex.Female);
      },
    );

    test(
      '- set role-specific data (Informed Consent for Father)',
      () async {
        final participation = CarpParticipationService().participation();

        final data = await participation.setParticipantData(
          {
            InformedConsentInput.type: InformedConsentInput(
              userId: 'ec44c84d-3acd-45d5-83ef-1511e0c39e48',
              name: father,
              consent: 'I agree!',
              signatureImage: 'blob',
            )
          },
          father,
        );
        debugPrint(toJsonString(data));

        final consent = data.roles
            .firstWhere((role) => role.roleName == father)
            .data[InformedConsentInput.type];

        expect(consent, isA<InformedConsentInput>());
        expect((consent as InformedConsentInput).name, father);
      },
    );

    test(
      '- set role-specific data (Full Name of Father)',
      () async {
        final participation = CarpParticipationService().participation();

        final data = await participation.setParticipantData(
          {
            FullNameInput.type: FullNameInput(
              firstName: 'Jakob',
              middleName: 'E.',
              lastName: 'Bardram',
            )
          },
          father,
        );
        debugPrint(toJsonString(data));

        final name = data.roles
            .firstWhere((role) => role.roleName == father)
            .data[FullNameInput.type];

        expect(name, isA<FullNameInput>());
        expect((name as FullNameInput).firstName, 'Jakob');
      },
    );

    test(
      '- get Informed Consent',
      () async {
        final participation = CarpParticipationService().participation();

        Map<String, InformedConsentInput?> consent =
            await participation.getInformedConsent();
        debugPrint(toJsonString(consent));

        expect(consent[father], isA<InformedConsentInput>());
        expect(consent[father]?.name, father);
        expect(consent[mother], isNull);
        expect(consent[child], isNull);
      },
    );

    test(
      '- set Informed Consent',
      () async {
        final participation = CarpParticipationService().participation();

        await participation.setInformedConsent(
          InformedConsentInput(
            userId: 'ec44c84d-3acd-45d5-83ef-1511e0c39e48',
            name: father,
            consent: 'I agree!',
            signatureImage: 'blob',
          ),
          father,
        );

        final consent = await participation.getInformedConsent();
        debugPrint(toJsonString(consent));

        expect(consent[father], isA<InformedConsentInput>());
        expect(consent[father]?.name, father);
        expect(consent[mother], isNull);
        expect(consent[child], isNull);
      },
    );

    test(
      '- remove Informed Consent',
      () async {
        final participation = CarpParticipationService().participation();

        await participation.removeInformedConsent(father);

        final consent = await participation.getInformedConsent();
        debugPrint(toJsonString(consent));

        expect(consent[father], isNull);
        expect(consent[mother], isNull);
        expect(consent[child], isNull);
      },
    );
  });
}
