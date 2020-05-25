const {MongoClient} = require('mongodb');
const stream = require('stream');

async function listDatabases(client){
  databasesList = await client.db().admin().listDatabases();

  console.log("Databases:");
  databasesList.databases.forEach(db => console.log(` - ${db.name}`));
};

function closeChangeStream(timeInMs = 60000, changeStream) {
  return new Promise((resolve) => {
      setTimeout(() => {
          console.log("Closing the change stream");
          changeStream.close();
          resolve();
      }, timeInMs)
  })
};

async function monitorListingsUsingEventEmitter(client, timeInMs = 60000, pipeline = []) {
  const collection = client.db("sample_airbnb").collection("listingsAndReviews");
  const changeStream = collection.watch(pipeline);
  changeStream.on('change', (next) => {
    console.log(next);
 });
  await closeChangeStream(timeInMs, changeStream);
}

async function main(){
  /**
   * Connection URI. Update <username>, <password>, and <your-cluster-url> to reflect your cluster.
   * See https://docs.mongodb.com/ecosystem/drivers/node/ for more details
   */
  const uri = 'mongodb+srv://dbUser:dbUserPassword@cluster0-jj6uu.mongodb.net/test?retryWrites=true&w=majority';


  const client = new MongoClient(uri);

  try {
      // Connect to the MongoDB cluster
      await client.connect();
      const pipeline = [
        {
		'$match': {
                'operationType': 'insert',
                // 'fullDocument.address.country': 'Australia',
                // 'fullDocument.address.market': 'Sydney'
            },
        }
    ];
      // Make the appropriate DB calls
      await  listDatabases(client);
      // await monitorListingsUsingEventEmitter(client, 30000, pipeline);
      // await monitorListingsUsingHasNext(client, 30000, pipeline);
      await monitorListingsUsingEventEmitter(client, 30000, pipeline);
  } catch (e) {
      console.error(e);
  } finally {
      await client.close();
  }
}

main().catch(console.error);
