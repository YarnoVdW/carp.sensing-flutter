/*
 * Copyright 2018-2022 Copenhagen Center for Health Technology (CACHET) at the
 * Technical University of Denmark (DTU).
 * Use of this source code is governed by a MIT-style license that can be
 * found in the LICENSE file.
 */
part of carp_services;

/// Provide a data endpoint reference to a CARP Web Service.
/// Used to:
/// - post (upload) a [DataPoint]
/// - batch upload a list or file of [DataPoint]s
/// - get a [DataPoint]
/// - query for [DataPoint]s
/// - delete a [DataPoint]
class DataPointReference extends CarpReference {
  DataPointReference._(CarpService service) : super._(service);

  /// The URL for the data end point for this [DataPointReference].
  String get dataEndpointUri =>
      "${service.app!.uri.toString()}/api/deployments/${service.app!.studyDeploymentId}/data-points";

  /// Uploads [data].
  ///
  /// Returns the server-generated ID for this data point.
  Future<int> post(DataPoint data) async {
    final String url = dataEndpointUri;

    // POST the data point to the CARP web service
    http.Response response = await httpr.post(Uri.encodeFull(url),
        headers: headers, body: json.encode(data));

    int httpStatusCode = response.statusCode;
    Map<String, dynamic> responseJson =
        json.decode(response.body) as Map<String, dynamic>;

    if ((httpStatusCode == HttpStatus.ok) ||
        (httpStatusCode == HttpStatus.created)) {
      return responseJson["id"] as int;
    }

    // All other cases are treated as an error.
    throw CarpServiceException(
      httpStatus: HTTPStatus(httpStatusCode, response.reasonPhrase),
      message: responseJson["message"].toString(),
      path: responseJson["path"].toString(),
    );
  }

  int _counter = 0;
  String? _fileCachePath;

  /// The base path for storing all file cache to be uploaded.
  Future<String> get fileCachePath async {
    if (_fileCachePath == null) {
      var path = 'cache';

      if (Settings().initialized) {
        final deploymentId = service.app?.studyDeploymentId ?? 'tmp';
        path = await Settings().getCacheBasePath(deploymentId);
      }
      final directory = await Directory('$path/upload').create(recursive: true);
      _fileCachePath = directory.path;
    }

    return _fileCachePath!;
  }

  // TODO - Delete cached file when uploaded.
  // Something like:
  //
  //   .then((file) => upload(file).then((_) => file.delete()));
  //
  // But this does not work, since this will delete the file before it is
  // uploaded, especially if there is a long retry cycle.
  // Need to listen to some sort of event that the file is successfully
  // uploaded, and then delete it.

  /// Batch upload a list of [DataPoint]s.
  ///
  /// Returns when successful. Throws an [CarpServiceException] if not.
  Future<void> batch(List<DataPoint> batch) async {
    if (batch.isEmpty) return;

    await File('${await fileCachePath}/batch-${_counter++}.json')
        .create(recursive: true)
        .then((file) => file.writeAsString(toJsonString(batch), flush: true))
        .then((file) => upload(file));
  }

  /// Batch upload a file with [DataPoint]s to the CARP backend using
  /// HTTP POST.
  ///
  /// A file can be created using a [FileDataManager] in `carp_mobile_sensing`.
  /// Note that the file should be raw JSON, and hence _not_ zipped.
  ///
  /// Returns when successful. Throws an [CarpServiceException] if not.
  Future<void> upload(File file) async {
    final String url = "$dataEndpointUri/batch";

    var request = http.MultipartRequest("POST", Uri.parse(url));
    request.headers['Authorization'] = headers['Authorization']!;
    request.headers['Content-Type'] = 'multipart/form-data';
    request.headers['cache-control'] = 'no-cache';

    request.files.add(ClonableMultipartFile.fromFileSync(file.path));

    // sending the request using the retry approach
    httpr.send(request).then((response) async {
      final int httpStatusCode = response.statusCode;

      // CARP web service returns 200 or 201 when a file is uploaded to the server
      if ((httpStatusCode == HttpStatus.ok) ||
          (httpStatusCode == HttpStatus.created)) return;

      // everything else is an exception
      response.stream.toStringStream().first.then((body) {
        final Map<String, dynamic> responseJson =
            json.decode(body) as Map<String, dynamic>;
        throw CarpServiceException(
          httpStatus: HTTPStatus(httpStatusCode, response.reasonPhrase),
          message: responseJson["message"].toString(),
          path: responseJson["path"].toString(),
        );
      });
    });
  }

  /// Get a [DataPoint] from the CARP backend using HTTP GET
  Future<DataPoint> get(int id) async {
    String url = "$dataEndpointUri/$id";

    // GET the data point from the CARP web service
    http.Response response =
        await httpr.get(Uri.encodeFull(url), headers: headers);

    int httpStatusCode = response.statusCode;
    Map<String, dynamic> responseJson =
        json.decode(response.body) as Map<String, dynamic>;

    if (httpStatusCode == HttpStatus.ok) {
      return DataPoint.fromJson(responseJson);
    }

    // All other cases are treated as an error.
    throw CarpServiceException(
      httpStatus: HTTPStatus(httpStatusCode, response.reasonPhrase),
      message: responseJson["message"].toString(),
      path: responseJson["path"].toString(),
    );
  }

  /// Get all [DataPoint]s for this study.
  ///
  /// Be careful using this method - this might potential return an enormous
  /// amount of data.
  Future<List<DataPoint>> getAll() async => query('');

  /// Query for [DataPoint]s from the CARP backend using
  /// [REST SQL (RSQL)](https://github.com/jirutka/rsql-parser).
  ///
  /// The [query] string can be build by querying data point _fields_ using
  /// _logical operations_.
  ///
  /// Query fields can be any field in a data point JSON, including nested fields.
  /// Examples include:
  ///
  ///  * Data point fields such as `id`, `study_id` and `created_at`.
  ///  * Header fields such as `carp_header.start_time`, `carp_header.user_id`, and `carp_header.data_format.name`
  ///  * Body fields such as `carp_body.latitude` or `carp_body.connectivity_status`
  ///
  /// Note that field names are nested using the dot-notation.
  ///
  /// See [here](https://github.com/jirutka/rsql-parser) for details on grammar and semantic.
  ///
  /// The logical operations include:
  ///
  ///   * Logical AND : `;` or `and`
  ///   * Logical OR : `,` or `or`
  ///
  /// Comparison operations include.
  ///
  ///   * Equal to : `==`
  ///   * Not equal to : `!=`
  ///   * Less than : `=lt=` or `<`
  ///   * Less than or equal to : `=le=` or `<=`
  ///   * Greater than operator : `=gt=` or `>`
  ///   * Greater than or equal to : `=ge=` or `>=`
  ///   * In : `=in=`
  ///   * Not in : `=out=`
  ///
  /// Examples of query strings include:
  ///
  /// Get all data-points between 2018-05-27T13:28:07 and 2019-05-29T08:55:26
  ///   * `carp_header.created_at>2018-05-27T13:28:07Z;carp_header.created_at<2019-05-29T08:55:26Z`
  ///
  /// Get all where the user id is 1 or 2
  ///   * `carp_header.user_id==1,2`
  ///
  ///
  /// Below is an example of a data point in JSON to see the different fields.
  ///
  /// ````
  /// {
  ///   "id": 24481799,
  ///   "study_id": 2,
  ///   "created_by_user_id": 2,
  ///   "created_at": "2019-06-19T09:50:44.245Z",
  ///   "updated_at": "2019-06-19T09:50:44.245Z",
  ///   "carp_header": {
  ///     "study_id": "8",
  ///     "user_id": "user@dtu.dk",
  ///     "data_format": {
  ///       "name": "location",
  ///       "namepace": "carp"
  ///     },
  ///     "trigger_id": "task1",
  ///     "device_role_name": "Patient's phone",
  ///     "upload_time": "2019-06-19T09:50:43.551Z",
  ///     "start_time": "2018-11-08T15:30:40.721748Z",
  ///     "end_time": "2019-06-19T09:50:43.551Z"
  ///   },
  ///   "carp_body": {
  ///     "altitude": 43.3,
  ///     "device_info": {},
  ///     "classname": "LocationDatum",
  ///     "latitude": 23454.345,
  ///     "accuracy": 12.4,
  ///     "speed_accuracy": 12.3,
  ///     "id": "3fdd1760-bd30-11e8-e209-ef7ee8358d2f",
  ///     "speed": 2.3,
  ///     "timestamp": "2018-11-08T15:30:40.721748Z",
  ///     "longitude": 23.4
  ///   }
  ///  }
  /// ````
  ///
  Future<List<DataPoint>> query(String query) async {
    String url =
        (query.isEmpty) ? dataEndpointUri : "$dataEndpointUri?query=$query";

    // GET the data points from the CARP web service
    // TODO - for some reason the CARP web service don't like encoded url's....
    // http.Response response = await httpr.get(Uri.encodeFull(url), headers: restHeaders);
    http.Response response = await httpr.get(url, headers: headers);

    int httpStatusCode = response.statusCode;

    if (httpStatusCode == HttpStatus.ok) {
      List<dynamic> list = json.decode(response.body) as List<dynamic>;
      List<DataPoint> datapoints = [];
      for (var item in list) {
        datapoints.add(DataPoint.fromJson(item as Map<String, dynamic>));
      }
      return datapoints;
    }
    // All other cases are treated as an error.
    Map<String, dynamic> responseJson =
        json.decode(response.body) as Map<String, dynamic>;
    throw CarpServiceException(
      httpStatus: HTTPStatus(httpStatusCode, response.reasonPhrase),
      message: responseJson["message"].toString(),
      path: responseJson["path"].toString(),
    );
  }

  /// The count of data points for this deployment.
  ///
  /// A [query] using [REST SQL (RSQL)](https://github.com/jirutka/rsql-parser)
  /// can be provided.
  Future<int> count([String query = '']) async {
    String url = (query.isEmpty)
        ? "$dataEndpointUri/count"
        : "$dataEndpointUri/count?query=$query";

    http.Response response = await httpr.get(url, headers: headers);
    int httpStatusCode = response.statusCode;

    if (httpStatusCode == HttpStatus.ok) {
      print('count response = ${response.body}');
      return int.tryParse(response.body) ?? 0;
    }
    // All other cases are treated as an error.
    Map<String, dynamic> responseJson =
        json.decode(response.body) as Map<String, dynamic>;
    throw CarpServiceException(
      httpStatus: HTTPStatus(httpStatusCode, response.reasonPhrase),
      message: responseJson["message"].toString(),
      path: responseJson["path"].toString(),
    );
  }

  /// Delete a data point with the given [id].
  /// Returns on success.
  /// Throws an exception if data point is not found or otherwise unsuccessful.
  Future<void> delete(int id) async {
    String url = "$dataEndpointUri/$id";

    http.Response response =
        await httpr.delete(Uri.encodeFull(url), headers: headers);
    final int httpStatusCode = response.statusCode;

    if (httpStatusCode == HttpStatus.ok) return;

    // All other cases are treated as an error.
    final Map<String, dynamic> responseJson =
        json.decode(response.body) as Map<String, dynamic>;
    throw CarpServiceException(
      httpStatus: HTTPStatus(httpStatusCode, response.reasonPhrase),
      message: responseJson["message"].toString(),
      path: responseJson["path"].toString(),
    );
  }
}
