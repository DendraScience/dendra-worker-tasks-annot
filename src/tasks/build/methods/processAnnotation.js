/**
 * Method to evaluate an annotation and trigger side-effects.
 */

const pick = require('lodash/pick')
const { getAuthUser } = require('../../../lib/helpers')

const SKIP_FIELDS = [
  'title',
  'description'
]
const SPEC_DEFAULTS = {
  annotation: {},
  annotation_before: {}
}

async function processAnnotation (req, ctx) {
  // TODO: Add more logging
  const {
    datastreamService,
    logger,
    skipMatching
  } = ctx
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec)
  const {
    annotation,
    annotation_before: annotationBefore
  } = spec

  /*
    Skip this request?
   */

  if (skipMatching(annotation, SKIP_FIELDS) || skipMatching(annotationBefore, SKIP_FIELDS)) {
    logger.warn('Skipping request', { _id: req._id })
    return {}
  }

  /*
    Authenticate and/or verify user credentials.
   */

  await getAuthUser(ctx)

  /*
    Touch datastreams that need their datapoints configuration built.
   */

  // TODO: Only do this if specific actions are referenced

  let datastreamIds = []
  let stationIds = []

  if (annotation.datastream_ids) datastreamIds.push(...annotation.datastream_ids)
  if (annotation.station_ids) stationIds.push(...annotation.station_ids)
  if (annotationBefore.datastream_ids) datastreamIds.push(...annotationBefore.datastream_ids)
  if (annotationBefore.station_ids) stationIds.push(...annotationBefore.station_ids)

  datastreamIds = [...new Set(datastreamIds)]
  stationIds = [...new Set(stationIds)]

  const query = {
    source_type: 'sensor',
    $or: [{
      _id: { $in: datastreamIds }
    }, {
      station_id: { $in: stationIds }
    }],
    $limit: 2000, // FIX: Implement unbounded find or pagination
    $select: ['_id'],
    $sort: {
      _id: 1 // ASC
    }
  }

  logger.info('Patching multiple datastreams', { datastreamIds, stationIds, query })

  const datastreamRes = await datastreamService.find({ query })
  for (const item of (datastreamRes.data || [])) {
    logger.info('Patching datastream', { _id: item._id })

    await datastreamService.patch(item._id, { $set: {
      source_type: 'sensor'
    } }) // Trigger rebuild
  }

  return datastreamRes
}

module.exports = async (...args) => {
  try {
    return await processAnnotation(...args)
  } catch (err) {
    // Wrap errors, ensure they are written to the store
    return {
      error: pick(err, ['code', 'className', 'message', 'type'])
    }
  }
}
