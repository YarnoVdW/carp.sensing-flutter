part of 'carp_backend.dart';

/// A Localization implementation that support loading localization from the
/// CARP web service.
class CarpLocalizations {
  final Locale locale;

  CarpLocalizations(this.locale);

  static CarpLocalizations? of(BuildContext context) {
    return Localizations.of<CarpLocalizations>(context, CarpLocalizations);
  }

  Map<String, String>? _localizedStrings;

  Future<void> load() async {
    _localizedStrings = await CarpResourceManager().getLocalizations(locale);
    if (_localizedStrings == null) {
      warning('Could not load localizations for locale: $locale');
    }
  }

  /// Get the translation for [key] to this [locale].
  /// If [key] is not translated, [key] is returned.
  String? translate(String key) =>
      (_localizedStrings!.containsKey(key)) ? _localizedStrings![key] : key;

  // Static member to have a simple access to the delegate from the MaterialApp
  static const LocalizationsDelegate<CarpLocalizations> delegate =
      _CarpLocalizationsDelegate();
}

class _CarpLocalizationsDelegate
    extends LocalizationsDelegate<CarpLocalizations> {
  // This delegate instance will never change (it doesn't even have fields!)
  // It can provide a constant constructor.
  const _CarpLocalizationsDelegate();

  @override
  bool isSupported(Locale locale) => CarpResourceManager().isSupported(locale);

  @override
  Future<CarpLocalizations> load(Locale locale) async {
    CarpLocalizations localizations = CarpLocalizations(locale);
    await localizations.load();
    return localizations;
  }

  @override
  bool shouldReload(_CarpLocalizationsDelegate old) => false;
}
