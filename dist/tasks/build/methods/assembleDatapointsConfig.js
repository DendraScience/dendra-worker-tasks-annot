"use strict";

/**
 * Method to assemble a datastream's datapoints configuration.
 */
const pick = require('lodash/pick');

const {
  getAuthUser
} = require('../../../lib/helpers');

const {
  DateTime,
  Interval
} = require('luxon'); // Reasonable min and max dates to perform low-level querying
// NOTE: Didn't use min/max integer since db date conversion could choke
// NOTE: Revised to be within InfluxDB default dates


const MIN_TIME = Date.UTC(1800, 1, 2);
const MAX_TIME = Date.UTC(2200, 1, 2);
const DATE_TIME_OPTS = {
  zone: 'utc'
};
const SPEC_DEFAULTS = {
  datastream: {}
};

function sortAndMerge(config) {
  const stack = []; // Efficiently merge config instances in a linear traversal

  config.sort((a, b) => {
    if (a.beginsAt < b.beginsAt) return -1;
    if (a.beginsAt > b.beginsAt) return 1;
    return 0;
  }).forEach(inst => {
    if (inst.endsBefore <= inst.beginsAt) {// Exclude: inverted interval
    } else if (stack.length === 0) {
      stack.push(inst); // Init stack
    } else {
      const top = stack[stack.length - 1];

      if (inst.beginsAt >= top.endsBefore) {
        stack.push(inst);
      } else if (inst.endsBefore <= top.endsBefore) {// Exclude: instance interval is within top interval
      } else if (inst.beginsAt === top.beginsAt) {
        stack.pop();
        stack.push(inst);
      } else {
        top.endsBefore = inst.beginsAt;
        stack.push(inst);
      }
    }
  });
  return stack;
} // function unwrapConfigInstances (config) {
//   return []
// }


function wrapConfigInstances(config) {
  return config.map(inst => {
    const beginsAt = DateTime.fromISO(inst.begins_at, DATE_TIME_OPTS);
    const endsBefore = DateTime.fromISO(inst.ends_before, DATE_TIME_OPTS);
    return {
      beginsAt: beginsAt.isValid ? beginsAt.toMillis() : MIN_TIME,
      endsBefore: endsBefore.isValid ? endsBefore.toMillis() : MAX_TIME,
      originalInst: inst
    };
  });
}

async function assembleDatapointsConfig(req, ctx) {
  // TODO: Add more logging
  const {
    annotationService,
    // datastreamService,
    logger,
    skipMatching
  } = ctx;
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec);
  const {
    datastream
  } = spec;
  /*
    Skip this request?
   */

  if (skipMatching(datastream.name) || skipMatching(datastream.description)) {
    logger.warn('Skipping request', {
      _id: req._id
    });
    return {};
  }
  /*
    Authenticate and/or verify user credentials.
   */


  await getAuthUser(ctx);
  /*
    Fetch relevant annotations.
   */

  let query = {
    enabled: true,
    $or: [{
      station_ids: datastream.station_id
    }, {
      datastream_ids: datastream._id
    }],
    $limit: 2000,
    // FIX: Implement unbounded find or pagination
    $sort: {
      _id: 1 // ASC

    }
  };
  logger.info('Finding annotations', {
    query
  });
  const annotRes = await annotationService.find({
    query
  });
  /*
    Build a datapoints configuration.
   */

  const annotations = annotRes.data || [];
  const minDateTime = DateTime.fromMillis(MIN_TIME, DATE_TIME_OPTS);
  const maxDateTime = DateTime.fromMillis(MAX_TIME, DATE_TIME_OPTS);
  let config = wrapConfigInstances(datastream.datapoints_config || []);
  logger.info(`Found (${annotations.length}) annotation(s)`);

  for (const annotation of annotations) {
    let beginsAt = DateTime.fromISO(annotation.begins_at);
    let endsBefore = DateTime.fromISO(annotation.ends_before);
    if (!beginsAt.isValid) beginsAt = minDateTime;
    if (!endsBefore.isValid) endsBefore = maxDateTime;
    const annotInterval = Interval.fromDateTimes(beginsAt, endsBefore);
    const stack = [];
    config = sortAndMerge(config);

    for (const inst of config) {
      const instInterval = Interval.fromDateTimes(DateTime.fromMillis(inst.beginsAt, DATE_TIME_OPTS), DateTime.fromMillis(inst.endsBefore, DATE_TIME_OPTS));

      if (annotInterval.overlaps(instInterval)) {// Consider exclude flag
      } else {
        stack.push(inst);
      }
    }
  } // TODO: Post process configs
  // TODO: Finish this!
  // TODO: Use a document version (like version_uuid) instead of updated_at!
  // const query = {
  //   _id: datastream._id,
  //   updated_at: datastream.updated_at
  // }
  // logger.info('Patching datastream', { query })
  // return datastreamService.patch(null, {
  //   datapoints_config_built: configBuilt
  // }, { query })


  return {};
}

module.exports = async (...args) => {
  try {
    return await assembleDatapointsConfig(...args);
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    };
  }
};