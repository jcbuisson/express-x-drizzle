import { and, eq, gt, gte, lt, lte, isNull, getTableName } from "drizzle-orm";

import { Mutex, truncateString, computeSyncResult } from '@jcbuisson/express-x'


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


// DOIT FAIRE UN ROLLBACK EN CAS D'ERREUR SERVEUR (EX: ERREUR SÉMANTIQUE TYPE PB CLÉ ÉTRANGÈRE OU AUTRE)


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
               const [meta] = await tx.update(metadata).set({ deleted_at: new Date(deleted_at) }).where(eq(metadata.uid, uid)).returning();
               return [value, meta]
            })
         },
      })
   }

   const syncMutexes = new Map()

   // add a synchronization service
   app.createService('sync', {

      // CUTOFFDATE INUTILE ?
      go: async (modelName, where, cutoffDate, clientMetadataDict) => {

         // get or create a mutex specific to modelName + where
         const mutexKey = `${modelName}:${JSON.stringify(Object.fromEntries(Object.entries(where).sort()))}`
         if (!syncMutexes.has(mutexKey)) syncMutexes.set(mutexKey, new Mutex())
         // acquire it: no other sync operation from another client on this model+where can occur in parallel
         await syncMutexes.get(mutexKey).acquire()

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
            syncMutexes.get(mutexKey).release()
         }
      },
   })

}
