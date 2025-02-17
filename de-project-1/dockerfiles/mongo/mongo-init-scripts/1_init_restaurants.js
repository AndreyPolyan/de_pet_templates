// Connect to the database
db = db.getSiblingDB("mongo_orders");

db.createCollection("restaurants", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name", "menu", "update_ts"],
      properties: {
        name: { bsonType: "string", description: "must be a string and is required" },
        menu: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["_id", "name", "price", "category"],
            properties: {
              _id: { bsonType: "objectId", description: "must be an ObjectId and is required" },
              name: { bsonType: "string", description: "must be a string and is required" },
              price: { bsonType: "double", description: "must be a number and is required" },
              category: { bsonType: "string", description: "must be a string and is required" },
            },
          },
        },
        update_ts: { bsonType: "date", description: "must be a date and is required" },
      },
    },
  },
});