-- SET search_path TO TPCC;
-- Condition 1: W_YTD = sum(D_YTD)
SELECT * FROM (SELECT w.w_id, w.w_ytd, d.sum_d_ytd
           FROM bmsql_warehouse w,
                (SELECT d_w_id, SUM(d_ytd) sum_d_ytd
                 FROM bmsql_district
                 GROUP BY d_w_id) d
           WHERE w.w_id = d.d_w_id) as x
WHERE w_ytd != sum_d_ytd;

-- Condition 2: D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
SELECT * FROM (SELECT d.d_w_id, d.d_id, d.d_next_o_id, o.max_o_id, no.max_no_o_id
           FROM bmsql_district d,
                (SELECT o_w_id, o_d_id, MAX(o_id) max_o_id
                 FROM bmsql_oorder
                 GROUP BY o_w_id, o_d_id) o,
                (SELECT no_w_id, no_d_id, MAX(no_o_id) max_no_o_id
                 FROM bmsql_new_order
                 GROUP BY no_w_id, no_d_id) no
           WHERE d.d_w_id = o.o_w_id AND d.d_w_id = no.no_w_id AND
                 d.d_id = o.o_d_id AND d.d_id = no.no_d_id) as x
WHERE d_next_o_id - 1 != max_o_id OR d_next_o_id - 1 != max_no_o_id;

-- Condition 3: max(NO_O_ID) - min(NO_O_ID) + 1
-- = [number of rows in the NEW-ORDER table for this bmsql_district]
SELECT * FROM (SELECT no_w_id, no_d_id, MAX(no_o_id) max_no_o_id,
                  MIN(no_o_id) min_no_o_id, COUNT(*) count_no
           FROM bmsql_new_order
           GROUP BY no_w_id, no_d_Id) as x
WHERE max_no_o_id - min_no_o_id + 1 != count_no;

-- Condition 4: sum(O_OL_CNT)
-- = [number of rows in the ORDER-LINE table for this bmsql_district]
SELECT * FROM (SELECT o.o_w_id, o.o_d_id, o.sum_o_ol_cnt, ol.count_ol
           FROM (SELECT o_w_id, o_d_id, SUM(o_ol_cnt) sum_o_ol_cnt
                 FROM bmsql_oorder
                 GROUP BY o_w_id, o_d_id) o,
                (SELECT ol_w_id, ol_d_id, COUNT(*) count_ol
                 FROM bmsql_order_line
                 GROUP BY ol_w_id, ol_d_id) ol
           WHERE o.o_w_id = ol.ol_w_id AND
                 o.o_d_id = ol.ol_d_id) as x
WHERE sum_o_ol_cnt != count_ol;

-- Condition 5: For any row in the ORDER table, O_CARRIER_ID is set to a null
-- value if and only if there is a corresponding row in the
-- NEW-ORDER table
SELECT * FROM (SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_carrier_id, no.count_no
           FROM bmsql_oorder o,
                (SELECT no_w_id, no_d_id, no_o_id, COUNT(*) count_no
                 FROM bmsql_new_order
                 GROUP BY no_w_id, no_d_id, no_o_id) no
           WHERE o.o_w_id = no.no_w_id AND
                 o.o_d_id = no.no_d_id AND
                 o.o_id = no.no_o_id) as x
WHERE (o_carrier_id IS NULL AND count_no = 0) OR
      (o_carrier_id IS NOT NULL AND count_no != 0);

-- Condition 6: For any row in the ORDER table, O_OL_CNT must equal the number
-- of rows in the ORDER-LINE table for the corresponding order
SELECT * FROM (SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, ol.count_ol
           FROM bmsql_oorder o,
                (SELECT ol_w_id, ol_d_id, ol_o_id, COUNT(*) count_ol
                 FROM bmsql_order_line
                 GROUP BY ol_w_id, ol_d_id, ol_o_id) ol
           WHERE o.o_w_id = ol.ol_w_id AND
                 o.o_d_id = ol.ol_d_id AND
                 o.o_id = ol.ol_o_id) as x
WHERE o_ol_cnt != count_ol;

-- Condition 7: For any row in the ORDER-LINE table, OL_DELIVERY_D is set to
-- a null date/time if and only if the corresponding row in the
-- ORDER table has O_CARRIER_ID set to a null value
SELECT * FROM (SELECT ol.ol_w_id, ol.ol_d_id, ol.ol_o_id, ol.ol_delivery_d,
                  o.o_carrier_id
           FROM bmsql_order_line ol,
                bmsql_oorder o
           WHERE ol.ol_w_id = o.o_w_id AND
                 ol.ol_d_id = o.o_d_id AND
                 ol.ol_o_id = o.o_id) as x
WHERE (ol_delivery_d IS NULL AND o_carrier_id IS NOT NULL) OR
      (ol_delivery_d IS NOT NULL AND o_carrier_id IS NULL);

-- Condition 8: W_YTD = sum(H_AMOUNT)
SELECT *
FROM (SELECT w.w_id, w.w_ytd, h.sum_h_amount
      FROM bmsql_warehouse w,
           (SELECT h_w_id, SUM(h_amount) sum_h_amount FROM bmsql_history GROUP BY h_w_id) h
      WHERE w.w_id = h.h_w_id) as x
WHERE w_ytd != sum_h_amount;

-- Condition 9: D_YTD = sum(H_AMOUNT)
SELECT *
FROM (SELECT d.d_w_id, d.d_id, d.d_ytd, h.sum_h_amount
      FROM bmsql_district d,
           (SELECT h_w_id, h_d_id, SUM(h_amount) sum_h_amount
            FROM bmsql_history
            GROUP BY h_w_id, h_d_id) h
      WHERE d.d_w_id = h.h_w_id
        AND d.d_id = h.h_d_id) as x
WHERE d_ytd != sum_h_amount;
