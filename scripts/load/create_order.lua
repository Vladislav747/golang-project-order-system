-- wrk POST /order
-- usage:
--   wrk -t4 -c50 -d30s -s scripts/load/create_order.lua http://127.0.0.1:8080

wrk.method = "POST"
wrk.path = "/order"
wrk.headers["Content-Type"] = "application/json"
wrk.body = [[{
  "customer_id": "6ba7b810-9dad-11d1-80b4-00c04fd43023",
  "status": "pending",
  "total_amount": 1500,
  "currency": "USD",
  "items": [{"sku":"A1","qty":1,"price":500},{"sku":"B2","qty":1,"price":500}]
}]]
