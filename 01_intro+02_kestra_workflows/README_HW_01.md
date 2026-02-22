# Module 1 Homework: Docker & SQL

## Question 1: Understanding Docker images

**Command:**
```bash
docker run -it --entrypoint bash python:3.13
pip --version
```

Answer: 25.3

---

## Question 2: Understanding Docker networking and docker-compose

Answer: db:5432

Inside Docker networks, containers communicate using the service name (`db`) and the internal container port (5432), not the host-mapped port (5433).

---

## Question 3: Counting short trips

SQL Query:
```sql
SELECT COUNT(*) 
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

Answer: 8,007

---

## Question 4: Longest trip for each day

SQL Query:
```sql
SELECT DATE(lpep_pickup_datetime) AS pickup_day, 
       MAX(trip_distance) AS max_distance
FROM green_taxi_trips
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;
```

Answer: 2025-11-14

---

## Question 5: Biggest pickup zone

SQL Query:
```sql
SELECT z."Zone", SUM(g.total_amount) AS total
FROM green_taxi_trips g
JOIN taxi_zones z ON g."PULocationID" = z."LocationID"
WHERE DATE(g.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total DESC
LIMIT 1;
```

Answer: East Harlem North

---

## Question 6: Largest tip

SQL Query:
```sql
SELECT z_dropoff."Zone", MAX(g.tip_amount) AS max_tip
FROM green_taxi_trips g
JOIN taxi_zones z_pickup ON g."PULocationID" = z_pickup."LocationID"
JOIN taxi_zones z_dropoff ON g."DOLocationID" = z_dropoff."LocationID"
WHERE z_pickup."Zone" = 'East Harlem North'
  AND g.lpep_pickup_datetime >= '2025-11-01'
  AND g.lpep_pickup_datetime < '2025-12-01'
GROUP BY z_dropoff."Zone"
ORDER BY max_tip DESC
LIMIT 1;
```

Answer: Yorkville West

---

## Question 7: Terraform Workflow

Answer: `terraform init, terraform apply -auto-approve, terraform destroy`