part of '../../carp_context_package.dart';

/// Holds mobility features information.
@JsonSerializable(fieldRename: FieldRename.none, includeIfNull: false)
class Mobility extends Data {
  static const dataType = ContextSamplingPackage.MOBILITY;

  /// The time this data was collected.
  DateTime? timestamp;

  /// The day of this mobility features.
  DateTime? date;

  /// Number of places visited on [date].
  int? numberOfPlaces;

  /// Location Variance on [date].
  double? locationVariance;

  /// Location entropy on [date].
  ///  * High entropy: Time is spent evenly among all places
  ///  * Low  entropy: Time is mainly spent at a few of the places
  double? entropy;

  /// Normalized entropy on [date]. A scalar between 0 and 1
  double? normalizedEntropy;

  /// Home Stay on [date]. A scalar between 0 and 1.
  double? homeStay;

  /// Distance traveled on [date], in meters.
  double? distanceTraveled;

  Mobility({
    this.timestamp,
    this.date,
    this.numberOfPlaces,
    this.locationVariance,
    this.entropy,
    this.normalizedEntropy,
    this.homeStay,
    this.distanceTraveled,
  }) : super() {
    timestamp ??= DateTime.now();
  }

  factory Mobility.fromMobilityContext(MobilityContext context) => Mobility()
    ..timestamp = context.timestamp
    ..date = context.date
    ..numberOfPlaces = context.numberOfSignificantPlaces
    ..locationVariance = context.locationVariance
    ..entropy = context.entropy
    ..normalizedEntropy = context.normalizedEntropy
    ..homeStay = context.homeStay
    ..distanceTraveled = context.distanceTravelled;

  @override
  Function get fromJsonFunction => _$MobilityFromJson;
  factory Mobility.fromJson(Map<String, dynamic> json) =>
      FromJsonFactory().fromJson(json) as Mobility;
  @override
  Map<String, dynamic> toJson() => _$MobilityToJson(this);
}
