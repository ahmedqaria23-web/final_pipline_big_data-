# MongoDB Explain('executionStats') Experiment Report

**Date**: 2026-10-04T16:53:03.776699+00:00  
**Database**: `ecommerce_store`  
**Collection**: `orders_validated`  
**Total Records Evaluated**: `1,791,633`  

## Summary of Indexes Tested

| Index Name | Type | Keys | Supported Query |
| :--- | :--- | :--- | :--- |
| `idx_customer_id` | **Single Field** | `customer.customer_id: 1` | `orders_by_customer` |
| `idx_city_status` | **Compound** | `customer.address.city: 1, status: 1` | `orders_by_city_and_status (and orders_by_city prefix)` |
| `idx_total_amount` | **Single Field** | `total_amount: -1` | `high_value_orders` |

## Experimental Results: Before vs. After Index Creation

### Query: `orders_by_customer`
* **Target Index**: `idx_customer_id`
* **Execution Description**: Lookup orders for customer_id='عميل-20'

| Metric | BEFORE Index | AFTER Index | Impact |
| :--- | :--- | :--- | :--- |
| **Winning Plan Stage** | `LIMIT -> COLLSCAN` | `LIMIT -> FETCH -> IXSCAN` | `LIMIT -> COLLSCAN -> LIMIT -> FETCH -> IXSCAN` |
| **Total Docs Examined** | `1,791,633` | `1` | **1,791,632 fewer docs scanned** |
| **Total Keys Examined** | `0` | `1` | `1 index keys read` |
| **Execution Time (ms)** | `2276 ms` | `0 ms` | `Δ 2276 ms` |
| **Returned Docs** | `1` | `1` | Exact same result count |

> **Interpretation**: Examined docs dropped from 1791633 to 1 (reduction of 1,791,632 docs). Plan transformed from LIMIT -> COLLSCAN to LIMIT -> FETCH -> IXSCAN.

### Query: `orders_by_city_and_status`
* **Target Index**: `idx_city_status`
* **Execution Description**: Compound lookup for city='ذمار' and status='قيد الشحن'

| Metric | BEFORE Index | AFTER Index | Impact |
| :--- | :--- | :--- | :--- |
| **Winning Plan Stage** | `LIMIT -> COLLSCAN` | `LIMIT -> FETCH -> IXSCAN` | `LIMIT -> COLLSCAN -> LIMIT -> FETCH -> IXSCAN` |
| **Total Docs Examined** | `22,583` | `50` | **22,533 fewer docs scanned** |
| **Total Keys Examined** | `0` | `50` | `50 index keys read` |
| **Execution Time (ms)** | `9 ms` | `9 ms` | `Δ 0 ms` |
| **Returned Docs** | `50` | `50` | Exact same result count |

> **Interpretation**: Examined docs dropped from 22583 to 50 (reduction of 22,533 docs). Plan transformed from LIMIT -> COLLSCAN to LIMIT -> FETCH -> IXSCAN.

### Query: `high_value_orders`
* **Target Index**: `idx_total_amount`
* **Execution Description**: High value range filter and sort total_amount >= 290500.0

| Metric | BEFORE Index | AFTER Index | Impact |
| :--- | :--- | :--- | :--- |
| **Winning Plan Stage** | `SORT -> COLLSCAN` | `LIMIT -> FETCH -> IXSCAN` | `SORT -> COLLSCAN -> LIMIT -> FETCH -> IXSCAN` |
| **Total Docs Examined** | `1,791,633` | `50` | **1,791,583 fewer docs scanned** |
| **Total Keys Examined** | `0` | `50` | `50 index keys read` |
| **Execution Time (ms)** | `2473 ms` | `7 ms` | `Δ 2466 ms` |
| **Returned Docs** | `50` | `50` | Exact same result count |

> **Interpretation**: Examined docs dropped from 1791633 to 50 (reduction of 1,791,583 docs). Plan transformed from SORT -> COLLSCAN to LIMIT -> FETCH -> IXSCAN.
