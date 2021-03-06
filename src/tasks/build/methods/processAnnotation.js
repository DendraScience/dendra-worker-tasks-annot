/**
 * Method to evaluate an annotation and trigger side-effects.
 */

const pick = require('lodash/pick')
const { getAuthUser } = require('../../../lib/helpers')

const SPEC_DEFAULTS = {
  annotation: {},
  annotation_before: {}
}

async function processAnnotation(req, ctx) {
  // TODO: Add more logging
  const { annotationService, datastreamService, logger } = ctx
  const spec = Object.assign({}, SPEC_DEFAULTS, req.spec)
  const { annotation, annotation_before: annotationBefore } = spec

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

  if (annotation.datastream_ids)
    datastreamIds.push(...annotation.datastream_ids)
  if (annotation.station_ids) stationIds.push(...annotation.station_ids)
  if (annotationBefore.datastream_ids)
    datastreamIds.push(...annotationBefore.datastream_ids)
  if (annotationBefore.station_ids)
    stationIds.push(...annotationBefore.station_ids)

  stationIds = [...new Set(stationIds)]

  logger.info('Finding datastreams for stations', { stationIds })

  // Get the datastreams for each station and add them to the list
  for (const stationId of stationIds) {
    const query = {
      source_type: 'sensor',
      station_id: stationId,
      $limit: 2000, // FIX: Implement unbounded find or pagination
      $select: ['_id'],
      $sort: {
        _id: 1 // ASC
      }
    }
    const datastreamRes = await datastreamService.find({ query })

    if (datastreamRes.data)
      datastreamRes.data.forEach(item => datastreamIds.push(item._id))
  }

  datastreamIds = [...new Set(datastreamIds)]

  logger.info('Patching multiple datastreams', { datastreamIds })

  for (const datastreamId of datastreamIds) {
    const query = {
      source_type: 'sensor'
    }

    logger.info('Patching datastream', { _id: datastreamId, query })

    const patchRes = await datastreamService.patch(
      datastreamId,
      {
        $set: {
          source_type: 'sensor'
        }
      },
      { query }
    ) // Trigger rebuild

    if (patchRes.station_id) stationIds.push(patchRes.station_id)
  }

  /*
    Patch the annotation with the affected stations.
   */

  stationIds = [...new Set(stationIds)]

  logger.info('Patching annotation', { _id: annotation._id })

  await annotationService.patch(annotation._id, {
    $set: {
      affected_station_ids: stationIds
    }
  })

  return { datastreamIds }
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
