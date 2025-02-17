// Connect to the database
db = db.getSiblingDB("mongo_orders");

// Create Orders collection
db.createCollection("orders", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["restaurant", "date", "user", "order_items", "final_status", "statuses", "update_ts"],
      properties: {
        restaurant: {
          bsonType: "object",
          required: ["id"],
          properties: {
            id: { bsonType: "objectId", description: "must be an ObjectId and is required" },
          },
        },
        date: { bsonType: "date", description: "must be a date and is required" },
        user: {
          bsonType: "object",
          required: ["id"],
          properties: {
            id: { bsonType: "objectId", description: "must be an ObjectId and is required" },
          },
        },
        order_items: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["id", "name", "price", "quantity"],
            properties: {
              id: { bsonType: "objectId", description: "must be an ObjectId and is required" },
              name: { bsonType: "string", description: "must be a string and is required" },
              price: { bsonType: "double", description: "must be a number and is required" },
              quantity: { bsonType: "int", description: "must be an integer and is required" },
            },
          },
        },
        statuses: {
          bsonType: "array",
          items:{
            bsonType: "object",
            required: ["status", "dttm"],
            properties: {
              status: { bsonType: "string", description: "must be a string and is required" },
              dttm: { bsonType: "date", description: "must be a date and is required" }
            }
          },
        },
        cost: { bsonType: "double", description: "must be a number and is required" },
        final_status: { bsonType: "string", description: "must be a string and is required" },
        update_ts: { bsonType: "date", description: "must be a date and is required" },
      },
    },
  },
});
