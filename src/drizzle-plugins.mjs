import { and, eq, gt, gte, lt, lte, isNull, getTableName } from "drizzle-orm";

import { truncateString, computeSyncResult } from '@jcbuisson/express-x'


//////////////////////////       UTILITIES       //////////////////////////

function whereToDrizzleFilters(table, where) {
   const conditions = Object.entries(where)
      .filter(([_, value]) => value !== undefined)
      .map(([key, value]) => {
         if (value === null) return isNull(table[key])
         if (typeof value === 'object') {
            // Collect ALL range bounds — a compound { gte:1, lte:10 } needs both
            const bounds = []
            if ('gte' in value) bounds.push(gte(table[key], value.gte))
            if ('gt'  in value) bounds.push(gt(table[key],  value.gt))
            if ('lte' in value) bounds.push(lte(table[key], value.lte))
            if ('lt'  in value) bounds.push(lt(table[key],  value.lt))
            if (bounds.length > 0) return and(...bounds)
         }
         return eq(table[key], value)
      })
   return conditions.length ? and(...conditions) : undefined;
}

function isPlainObject(value) {
   return value && typeof value === 'object' && !Array.isArray(value) && Object.prototype.toString.call(value) === '[object Object]'
}

function hasRangeOperator(value) {
   return isPlainObject(value) && ['gte', 'gt', 'lte', 'lt'].some(key => key in value)
}

function valueWithinRange(value, range) {
   if (value === null || value === undefined) return false
   if ('gte' in range && value < range.gte) return false
   if ('gt' in range && value <= range.gt) return false
   if ('lte' in range && value > range.lte) return false
   if ('lt' in range && value >= range.lt) return false
   return true
}

function rangesOverlap(a, b) {
   const lower = [
      'gte' in a && { value: a.gte, inclusive: true },
      'gt' in a && { value: a.gt, inclusive: false },
      'gte' in b && { value: b.gte, inclusive: true },
      'gt' in b && { value: b.gt, inclusive: false },
   ].filter(Boolean).sort((x, y) => x.value === y.value ? Number(x.inclusive) - Number(y.inclusive) : x.value > y.value ? -1 : 1)[0]

   const upper = [
      'lte' in a && { value: a.lte, inclusive: true },
      'lt' in a && { value: a.lt, inclusive: false },
      'lte' in b && { value: b.lte, inclusive: true },
      'lt' in b && { value: b.lt, inclusive: false },
   ].filter(Boolean).sort((x, y) => x.value === y.value ? Number(x.inclusive) - Number(y.inclusive) : x.value < y.value ? -1 : 1)[0]

   if (!lower || !upper) return true
   if (lower.value < upper.value) return true
   if (lower.value > upper.value) return false
   return lower.inclusive && upper.inclusive
}

function constraintsOverlap(a, b) {
   if (a === undefined || b === undefined) return true
   if (a === null || b === null) return a === null && b === null

   const aRange = hasRangeOperator(a)
   const bRange = hasRangeOperator(b)

   if (aRange && bRange) return rangesOverlap(a, b)
   if (aRange) return valueWithinRange(b, a)
   if (bRange) return valueWithinRange(a, b)
   if (isPlainObject(a) || isPlainObject(b)) return true
   return a === b
}

function whereScopesOverlap(a = {}, b = {}) {
   const sharedKeys = Object.keys(a).filter(key => key in b)
   for (const key of sharedKeys) {
      if (!constraintsOverlap(a[key], b[key])) return false
   }
   return true
}

class OverlapLock {
   constructor() {
      this.active = []
      this.queue = []
   }

   acquire(where) {
      return new Promise(resolve => {
         const entry = { where, resolve }
         this.queue.push(entry)
         this.pump()
      })
   }

   pump() {
      for (let i = 0; i < this.queue.length; i++) {
         const entry = this.queue[i]
         if (this.active.some(active => whereScopesOverlap(active.where, entry.where))) continue
         this.queue.splice(i, 1)
         i -= 1
         this.active.push(entry)
         entry.resolve(() => {
            this.active = this.active.filter(active => active !== entry)
            this.pump()
         })
      }
   }

   get idle() {
      return this.active.length === 0 && this.queue.length === 0
   }
}


//////////////////////////       DRIZZLE OFFLINE PLUGIN       //////////////////////////

export function drizzleOfflinePlugin(app, db, metadata, models) {

   // add a database service for each model
   for (const model of models) {
      const modelName = getTableName(model)

      app.createService(modelName, {

         findUnique: async (where) => {
            const rows = await db.select().from(model).where(whereToDrizzleFilters(model, where));
            return rows[0] ?? null;
         },

         findMany: async (where) => {
            return await db.select().from(model).where(whereToDrizzleFilters(model, where));
         },
         
         createWithMeta: async (uid, data, created_at) => {
            const ts = new Date(created_at)
            return await db.transaction(async (tx) => {
               // Upsert: if the model row already exists (e.g. a concurrent createWithMeta
               // from the direct create() path landed before the sync's addDatabase step),
               // update it instead of throwing a PK conflict that would rollback the
               // client's Dexie record.
               const [value] = await tx.insert(model)
                  .values({ uid, ...data })
                  .onConflictDoUpdate({ target: model.uid, set: data })
                  .returning();
               // Upsert metadata: handles re-creation after a prior deleteWithMeta.
               const [meta] = await tx.insert(metadata)
                  .values({ uid, created_at: ts })
                  .onConflictDoUpdate({
                     target: metadata.uid,
                     set: { created_at: ts, deleted_at: null, updated_at: null },
                  })
                  .returning();
               return [value, meta]
            })
         },

         updateWithMeta: async (uid, data, updated_at) => {
            const ts = updated_at ? new Date(updated_at) : null
            return await db.transaction(async (tx) => {
               const [value] = await tx.update(model).set(data).where(eq(model.uid, uid)).returning();
               // Upsert metadata: if the row is missing (data-integrity gap where the
               // model row exists but no metadata row), create it so the loop stops.
               const [meta] = await tx.insert(metadata)
                  .values({ uid, updated_at: ts })
                  .onConflictDoUpdate({ target: metadata.uid, set: { updated_at: ts } })
                  .returning();
               return [value, meta]
            })
         },

         deleteWithMeta: async (uid, deleted_at) => {
            return await db.transaction(async (tx) => {
               const [value] = await tx.delete(model).where(eq(model.uid, uid)).returning();
               const ts = new Date(deleted_at)
               const [meta] = await tx.insert(metadata)
                  .values({ uid, deleted_at: ts })
                  .onConflictDoUpdate({ target: metadata.uid, set: { deleted_at: ts } })
                  .returning();
               return [value, meta]
            })
         },
      })
   }

   const syncLocks = new Map()

   // add a synchronization service
   app.createService('sync', {

      // CUTOFFDATE INUTILE ?
      go: async (modelName, where, cutoffDate, clientMetadataDict) => {

         // overlap-aware lock so independent scopes can still run in parallel, but overlapping where predicates do not
         if (!syncLocks.has(modelName)) syncLocks.set(modelName, new OverlapLock())
         const syncLock = syncLocks.get(modelName)
         const releaseSyncLock = await syncLock.acquire(where)

         try {
            console.log('>>>>> SYNC', modelName, where, cutoffDate)
            const databaseService = app.service(modelName)
      
            // STEP1: get existing database `where` values and build a dictionary
            const databaseValues = await databaseService.findMany(where)
            const databaseValuesDict = databaseValues.reduce((accu, value) => {
               accu[value.uid] = value
               return accu
            }, {})

            // STEP 2: fetch metadata for each database record
            const databaseMetadataDict = {}
            for (const uid of Object.keys(databaseValuesDict)) {
               const meta = (await db.select().from(metadata).where(eq(metadata.uid, uid)))[0] ?? null
               if (meta) databaseMetadataDict[uid] = meta
            }

            // STEP 3: compute sync result
            const result = computeSyncResult(databaseValuesDict, clientMetadataDict, databaseMetadataDict)

            // STEP 4: execute server-side deletions
            for (const uid of result.deleteDatabase) {
               await databaseService.deleteWithMeta(uid, clientMetadataDict[uid].deleted_at)
            }

            console.log('addDatabase', truncateString(JSON.stringify(result.addDatabase)))
            console.log('updateDatabase', truncateString(JSON.stringify(result.updateDatabase)))
            console.log('addClient', truncateString(JSON.stringify(result.addClient)))
            console.log('deleteClient', truncateString(JSON.stringify(result.deleteClient)))
            console.log('updateClient', truncateString(JSON.stringify(result.updateClient)))

            return {
               addClient: result.addClient,
               updateClient: result.updateClient,
               deleteClient: result.deleteClient,
               addDatabase: result.addDatabase,
               updateDatabase: result.updateDatabase,
            }
         } catch(err) {
            console.log('*** err sync', err)
            throw err
         } finally {
            releaseSyncLock()
            if (syncLock.idle) syncLocks.delete(modelName)
         }
      },
   })

}
