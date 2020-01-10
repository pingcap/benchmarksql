/*
 * LoadDataWorker - Class to load one Warehouse (or in a special case
 * the ITEM table).
 *
 * Copyright (C) 2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 */

import java.sql.*;
import java.util.*;
import java.io.*;

public class LoadDataWorker implements Runnable
{
    private int                 worker;
    private Connection          dbConn;
    private jTPCCRandom         rnd;

    private StringBuffer        sb;
    private Formatter           fmt;

    private boolean             writeCSV = false;
    private String              csvNull = null;

    private PreparedStatement   stmtConfig = null;
    private PreparedStatement   stmtItem = null;
    private PreparedStatement   stmtWarehouse = null;
    private PreparedStatement   stmtDistrict = null;
    private PreparedStatement   stmtStock = null;
    private PreparedStatement   stmtCustomer = null;
    private PreparedStatement   stmtHistory = null;
    private PreparedStatement   stmtOrder = null;
    private PreparedStatement   stmtOrderLine = null;
    private PreparedStatement   stmtNewOrder = null;

    private StringBuffer        sbConfig = null;
    private Formatter           fmtConfig = null;
    private StringBuffer        sbItem = null;
    private Formatter           fmtItem = null;
    private StringBuffer        sbWarehouse = null;
    private Formatter           fmtWarehouse = null;
    private StringBuffer        sbDistrict = null;
    private Formatter           fmtDistrict = null;
    private StringBuffer        sbStock = null;
    private Formatter           fmtStock = null;
    private StringBuffer        sbCustomer = null;
    private Formatter           fmtCustomer = null;
    private StringBuffer        sbHistory = null;
    private Formatter           fmtHistory = null;
    private StringBuffer        sbOrder = null;
    private Formatter           fmtOrder = null;
    private StringBuffer        sbOrderLine = null;
    private Formatter           fmtOrderLine = null;
    private StringBuffer        sbNewOrder = null;
    private Formatter           fmtNewOrder = null;

    LoadDataWorker(int worker, String csvNull, jTPCCRandom rnd)
    {
	this.worker             = worker;
	this.csvNull            = csvNull;
	this.rnd                = rnd;

	this.sb                 = new StringBuffer();
	this.fmt                = new Formatter(sb);
	this.writeCSV           = true;

	this.sbConfig           = new StringBuffer();
	this.fmtConfig          = new Formatter(sbConfig);
	this.sbItem             = new StringBuffer();
	this.fmtItem            = new Formatter(sbItem);
	this.sbWarehouse        = new StringBuffer();
	this.fmtWarehouse       = new Formatter(sbWarehouse);
	this.sbDistrict         = new StringBuffer();
	this.fmtDistrict        = new Formatter(sbDistrict);
	this.sbStock            = new StringBuffer();
	this.fmtStock           = new Formatter(sbStock);
	this.sbCustomer         = new StringBuffer();
	this.fmtCustomer        = new Formatter(sbCustomer);
	this.sbHistory          = new StringBuffer();
	this.fmtHistory         = new Formatter(sbHistory);
	this.sbOrder            = new StringBuffer();
	this.fmtOrder           = new Formatter(sbOrder);
	this.sbOrderLine        = new StringBuffer();
	this.fmtOrderLine       = new Formatter(sbOrderLine);
	this.sbNewOrder         = new StringBuffer();
	this.fmtNewOrder        = new Formatter(sbNewOrder);
    }

    LoadDataWorker(int worker, Connection dbConn, jTPCCRandom rnd)
	throws SQLException
    {
	this.worker     = worker;
	this.dbConn     = dbConn;
	this.rnd        = rnd;

	this.sb         = new StringBuffer();
	this.fmt        = new Formatter(sb);

	stmtConfig = dbConn.prepareStatement(
		"INSERT INTO bmsql_config (" +
		"  cfg_name, cfg_value) " +
		"VALUES (?, ?)"
	    );
	stmtItem = dbConn.prepareStatement(
		"INSERT INTO bmsql_item (" +
		"  i_id, i_name, i_price, i_data, i_im_id) " +
		"VALUES (?, ?, ?, ?, ?)"
	    );
	stmtWarehouse = dbConn.prepareStatement(
		"INSERT INTO bmsql_warehouse (" +
		"  w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, " +
		"  w_state, w_zip) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	    );
	stmtStock = dbConn.prepareStatement(
		"INSERT INTO bmsql_stock ("+
		"  s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01, s_dist_02, " +
		"  s_dist_03, s_dist_04, s_dist_05, s_dist_06, " +
		"  s_dist_07, s_dist_08, s_dist_09, s_dist_10) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	    );
	stmtDistrict = dbConn.prepareStatement(
		"INSERT INTO bmsql_district ("+
		"  d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, " +
		"  d_city, d_state, d_zip) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	    );
	stmtCustomer = dbConn.prepareStatement(
		"INSERT INTO bmsql_customer (" +
		"  c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_credit_lim, " +
		"  c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, " +
		"  c_street_1, c_street_2, c_city, c_state, c_zip, " +
		"  c_phone, c_since, c_middle, c_data) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
		"        ?, ?, ?, ?, ?, ?)"
	    );
	stmtHistory = dbConn.prepareStatement(
		"INSERT INTO bmsql_history (" +
		"  hist_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, " +
		"  h_date, h_amount, h_data) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	    );
	stmtOrder = dbConn.prepareStatement(
		"INSERT INTO bmsql_oorder (" +
		"  o_w_id, o_d_id, o_id, o_c_id, " +
		"  o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	    );
	stmtOrderLine = dbConn.prepareStatement(
		"INSERT INTO bmsql_order_line (" +
		"  ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, " +
		"  ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, " +
		"  ol_dist_info) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	    );
	stmtNewOrder = dbConn.prepareStatement(
		"INSERT INTO bmsql_new_order (" +
		"  no_w_id, no_d_id, no_o_id) " +
		"VALUES (?, ?, ?)"
	    );
    }

    /*
     * run()
     */
    public void run()
    {
	int     job;

	try
	{
	    while ((job = LoadData.getNextJob()) >= 0)
	    {
		if (job == 0)
		{
		    fmt.format("Worker %03d: Loading ITEM", worker);
		    System.out.println(sb.toString());
		    sb.setLength(0);

		    loadItem();

		    fmt.format("Worker %03d: Loading ITEM done", worker);
		    System.out.println(sb.toString());
		    sb.setLength(0);
		}
		else
		{
		    fmt.format("Worker %03d: Loading Warehouse %6d",
			       worker, job);
		    System.out.println(sb.toString());
		    sb.setLength(0);

		    loadWarehouse(job);

		    fmt.format("Worker %03d: Loading Warehouse %6d done",
			       worker, job);
		    System.out.println(sb.toString());
		    sb.setLength(0);
		}
	    }

	    /*
	     * Close the DB connection if in direct DB mode.
	     */
	    if (!writeCSV)
		dbConn.close();
	}
	catch (SQLException se)
	{
	    while (se != null)
	    {
		fmt.format("Worker %03d: ERROR: %s", worker, se.getMessage());
		System.err.println(sb.toString());
		sb.setLength(0);
		se = se.getNextException();
	    }
	}
	catch (Exception e)
	{
	    fmt.format("Worker %03d: ERROR: %s", worker, e.getMessage());
	    System.err.println(sb.toString());
	    sb.setLength(0);
	    e.printStackTrace();
	    return;
	}
    } // End run()

    /* ----
     * loadItem()
     *
     * Load the content of the ITEM table.
     * ----
     */
    private void loadItem()
	throws SQLException, IOException
    {
	int                     i_id;

	if (writeCSV)
	{
	    /*
	     * Saving CONFIG information in CSV mode.
	     */
	    fmtConfig.format("warehouses,%d\n", LoadData.getNumWarehouses());
	    fmtConfig.format("nURandCLast,%d\n", rnd.getNURandCLast());
	    fmtConfig.format("nURandCC_ID,%d\n", rnd.getNURandCC_ID());
	    fmtConfig.format("nURandCI_ID,%d\n", rnd.getNURandCI_ID());

	    LoadData.configAppend(sbConfig);
	}
	else
	{
	    /*
	     * Saving CONFIG information in DB mode.
	     */
	    stmtConfig.setString(1, "warehouses");
	    stmtConfig.setString(2, "" + LoadData.getNumWarehouses());
	    stmtConfig.execute();

	    stmtConfig.setString(1, "nURandCLast");
	    stmtConfig.setString(2, "" + rnd.getNURandCLast());
	    stmtConfig.execute();

	    stmtConfig.setString(1, "nURandCC_ID");
	    stmtConfig.setString(2, "" + rnd.getNURandCC_ID());
	    stmtConfig.execute();

	    stmtConfig.setString(1, "nURandCI_ID");
	    stmtConfig.setString(2, "" + rnd.getNURandCI_ID());
	    stmtConfig.execute();
	}

	for (i_id = 1; i_id <= 100000; i_id++)
	{
	    String iData;

	    if (i_id != 1 && (i_id - 1) % 100 == 0)
	    {
		if (writeCSV)
		{
		    LoadData.itemAppend(sbItem);
		}
		else
		{
		    stmtItem.executeBatch();
		    stmtItem.clearBatch();
		    dbConn.commit();
		}
	    }

	    // Clause 4.3.3.1 for ITEM
	    if (rnd.nextInt(1, 100) <= 10)
	    {
		int     len = rnd.nextInt(26, 50);
		int     off = rnd.nextInt(0, len - 8);

		iData = rnd.getAString(off, off) +
			"ORIGINAL" +
			rnd.getAString(len - off - 8, len - off - 8);
	    }
	    else
	    {
		iData = rnd.getAString(26, 50);
	    }

	    if (writeCSV)
	    {
		fmtItem.format("%d,%s,%.2f,%s,%d\n",
			i_id,
			rnd.getAString(14, 24),
			((double)rnd.nextLong(100, 10000)) / 100.0,
			iData,
			rnd.nextInt(1, 10000));

	    }
	    else
	    {
		stmtItem.setInt(1, i_id);
		stmtItem.setString(2, rnd.getAString(14, 24));
		stmtItem.setDouble(3, ((double)rnd.nextLong(100, 10000)) / 100.0);
		stmtItem.setString(4, iData);
		stmtItem.setInt(5, rnd.nextInt(1, 10000));

		stmtItem.addBatch();
	    }
	}

	if (writeCSV)
	{
	    LoadData.itemAppend(sbItem);
	}
	else
	{
	    stmtItem.executeBatch();
	    stmtItem.clearBatch();
	    stmtItem.close();

	    dbConn.commit();
	}

    } // End loadItem()

    /* ----
     * loadWarehouse()
     *
     * Load the content of one warehouse.
     * ----
     */
    private void loadWarehouse(int w_id)
	throws SQLException, IOException
    {
	/*
	 * Load the WAREHOUSE row.
	 */
	if (writeCSV)
	{
	    fmtWarehouse.format("%d,%.2f,%.4f,%s,%s,%s,%s,%s,%s\n",
		w_id,
		300000.0,
		((double)rnd.nextLong(0, 2000)) / 10000.0,
		rnd.getAString(6, 10),
		rnd.getAString(10, 20),
		rnd.getAString(10, 20),
		rnd.getAString(10, 20),
		rnd.getState(),
		rnd.getNString(4, 4) + "11111");

		LoadData.warehouseAppend(sbWarehouse);
	}
	else
	{
	    stmtWarehouse.setInt(1, w_id);
	    stmtWarehouse.setDouble(2, 300000.0);
	    stmtWarehouse.setDouble(3, ((double)rnd.nextLong(0, 2000)) / 10000.0);
	    stmtWarehouse.setString(4, rnd.getAString(6, 10));
	    stmtWarehouse.setString(5, rnd.getAString(10, 20));
	    stmtWarehouse.setString(6, rnd.getAString(10, 20));
	    stmtWarehouse.setString(7, rnd.getAString(10, 20));
	    stmtWarehouse.setString(8, rnd.getState());
	    stmtWarehouse.setString(9, rnd.getNString(4, 4) + "11111");

	    stmtWarehouse.execute();
	}

	/*
	 * For each WAREHOUSE there are 100,000 STOCK rows.
	 */
	for (int s_i_id = 1; s_i_id <= 100000; s_i_id++)
	{
	    String sData;
	    /*
	     * Load the data in batches of 500 rows.
	     */
	    if (s_i_id != 1 && (s_i_id - 1) % 500 == 0)
	    {
		if (writeCSV)
		    LoadData.warehouseAppend(sbWarehouse);
		else
		{
		    stmtStock.executeBatch();
		    stmtStock.clearBatch();
		    dbConn.commit();
		}
	    }

	    // Clause 4.3.3.1 for STOCK
	    if (rnd.nextInt(1, 100) <= 10)
	    {
		int     len = rnd.nextInt(26, 50);
		int     off = rnd.nextInt(0, len - 8);

		sData = rnd.getAString(off, off) +
			"ORIGINAL" +
			rnd.getAString(len - off - 8, len - off - 8);
	    }
	    else
	    {
		sData = rnd.getAString(26, 50);
	    }

	    if (writeCSV)
	    {
		fmtStock.format("%d,%d,%d,%d,%d,%d,%s," +
				"%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
				w_id,
				s_i_id,
				rnd.nextInt(10, 100),
				0,
				0,
				0,
				sData,
				rnd.getAString(24, 24),
				rnd.getAString(24, 24),
				rnd.getAString(24, 24),
				rnd.getAString(24, 24),
				rnd.getAString(24, 24),
				rnd.getAString(24, 24),
				rnd.getAString(24, 24),
				rnd.getAString(24, 24),
				rnd.getAString(24, 24),
				rnd.getAString(24, 24));
	    }
	    else
	    {
		stmtStock.setInt(1, w_id);
		stmtStock.setInt(2, s_i_id);
		stmtStock.setInt(3, rnd.nextInt(10, 100));
		stmtStock.setInt(4, 0);
		stmtStock.setInt(5, 0);
		stmtStock.setInt(6, 0);
		stmtStock.setString(7, sData);
		stmtStock.setString(8, rnd.getAString(24, 24));
		stmtStock.setString(9, rnd.getAString(24, 24));
		stmtStock.setString(10, rnd.getAString(24, 24));
		stmtStock.setString(11, rnd.getAString(24, 24));
		stmtStock.setString(12, rnd.getAString(24, 24));
		stmtStock.setString(13, rnd.getAString(24, 24));
		stmtStock.setString(14, rnd.getAString(24, 24));
		stmtStock.setString(15, rnd.getAString(24, 24));
		stmtStock.setString(16, rnd.getAString(24, 24));
		stmtStock.setString(17, rnd.getAString(24, 24));

		stmtStock.addBatch();
	    }

	}
	if (writeCSV)
	{
	    LoadData.stockAppend(sbStock);
	}
	else
	{
	    stmtStock.executeBatch();
	    stmtStock.clearBatch();
	    dbConn.commit();
	}

	/*
	 * For each WAREHOUSE there are 10 DISTRICT rows.
	 */
	for (int d_id = 1; d_id <= 10; d_id++)
	{
	    if (writeCSV)
	    {
		fmtDistrict.format("%d,%d,%.2f,%.4f,%d,%s,%s,%s,%s,%s,%s\n",
			w_id,
			d_id,
			30000.0,
			((double)rnd.nextLong(0, 2000)) / 10000.0,
			3001,
			rnd.getAString(6, 10),
			rnd.getAString(10, 20),
			rnd.getAString(10, 20),
			rnd.getAString(10, 20),
			rnd.getState(),
			rnd.getNString(4, 4) + "11111");

		LoadData.districtAppend(sbDistrict);
	    }
	    else
	    {
		stmtDistrict.setInt(1, w_id);
		stmtDistrict.setInt(2, d_id);
		stmtDistrict.setDouble(3, 30000.0);
		stmtDistrict.setDouble(4, ((double)rnd.nextLong(0, 2000)) / 10000.0);
		stmtDistrict.setInt(5, 3001);
		stmtDistrict.setString(6, rnd.getAString(6, 10));
		stmtDistrict.setString(7, rnd.getAString(10, 20));
		stmtDistrict.setString(8, rnd.getAString(10, 20));
		stmtDistrict.setString(9, rnd.getAString(10, 20));
		stmtDistrict.setString(10, rnd.getState());
		stmtDistrict.setString(11, rnd.getNString(4, 4) + "11111");

		stmtDistrict.execute();
	    }

	    /*
	     * Within each DISTRICT there are 3,000 CUSTOMERs.
	     */
	    for (int c_id = 1; c_id <= 3000; c_id++)
	    {
			// commit district and history when 200 records
			if (c_id != 1 && (c_id - 1) % 200 == 0)
			{
				if (writeCSV){
					LoadData.customerAppend(sbCustomer);
					LoadData.historyAppend(sbHistory);
				}
				else
				{
					stmtCustomer.executeBatch();
					stmtCustomer.clearBatch();
					dbConn.commit();

					stmtHistory.executeBatch();
					stmtHistory.clearBatch();
					dbConn.commit();
				}
			}

		if (writeCSV)
		{
		    fmtCustomer.format("%d,%d,%d,%.4f,%s,%s,%s," +
			"%.2f,%.2f,%.2f,%d,%d," +
			"%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
			w_id,
			d_id,
			c_id,
			((double)rnd.nextLong(0, 5000)) / 10000.0,
			(rnd.nextInt(1, 100) <= 90) ? "GC" : "BC",
			(c_id <= 1000) ? rnd.getCLast(c_id - 1) : rnd.getCLast(),
			rnd.getAString(8, 16),
			50000.00,
			-10.00,
			10.00,
			1,
			1,
			rnd.getAString(10, 20),
			rnd.getAString(10, 20),
			rnd.getAString(10, 20),
			rnd.getState(),
			rnd.getNString(4, 4) + "11111",
			rnd.getNString(16, 16),
			new java.sql.Timestamp(System.currentTimeMillis()).toString(),
			"OE",
			rnd.getAString(300, 500));
		}
		else
		{
			stmtCustomer.setInt(1, w_id);
		    stmtCustomer.setInt(2, d_id);
			stmtCustomer.setInt(3, c_id);
			stmtCustomer.setDouble(4, ((double)rnd.nextLong(0, 5000)) / 10000.0);
			if (rnd.nextInt(1, 100) <= 90)
				stmtCustomer.setString(5, "GC");
			else
				stmtCustomer.setString(5, "BC");
			if (c_id <= 1000)
				stmtCustomer.setString(6, rnd.getCLast(c_id - 1));
			else
				stmtCustomer.setString(6, rnd.getCLast());
			stmtCustomer.setString(7, rnd.getAString(8, 16));
			stmtCustomer.setDouble(8, 50000.00);
			stmtCustomer.setDouble(9, -10.00);
			stmtCustomer.setDouble(10, 10.00);
			stmtCustomer.setInt(11, 1);
			stmtCustomer.setInt(12, 1);
			stmtCustomer.setString(13, rnd.getAString(10, 20));
			stmtCustomer.setString(14, rnd.getAString(10, 20));
			stmtCustomer.setString(15, rnd.getAString(10, 20));
			stmtCustomer.setString(16, rnd.getState());
			stmtCustomer.setString(17, rnd.getNString(4, 4) + "11111");
			stmtCustomer.setString(18, rnd.getNString(16, 16));
			stmtCustomer.setTimestamp(19, new java.sql.Timestamp(System.currentTimeMillis()));
			stmtCustomer.setString(20, "OE");
		    stmtCustomer.setString(21, rnd.getAString(300, 500));

		    stmtCustomer.addBatch();
		}

		/*
		 * For each CUSTOMER there is one row in HISTORY.
		 */
		if (writeCSV)
		{
		    fmtHistory.format("%d,%d,%d,%d,%d,%d,%s,%.2f,%s\n",
			(w_id - 1) * 30000 + (d_id - 1) * 3000 + c_id,
			c_id,
			d_id,
			w_id,
			d_id,
			w_id,
			new java.sql.Timestamp(System.currentTimeMillis()).toString(),
			10.00,
			rnd.getAString(12, 24));
		}
		else
		{
		    stmtHistory.setInt(1, (w_id - 1) * 30000 + (d_id - 1) * 3000 + c_id);
		    stmtHistory.setInt(2, c_id);
		    stmtHistory.setInt(3, d_id);
		    stmtHistory.setInt(4, w_id);
		    stmtHistory.setInt(5, d_id);
		    stmtHistory.setInt(6, w_id);
		    stmtHistory.setTimestamp(7, new java.sql.Timestamp(System.currentTimeMillis()));
		    stmtHistory.setDouble(8, 10.00);
		    stmtHistory.setString(9, rnd.getAString(12, 24));

		    stmtHistory.addBatch();
		}
	    }

	    if (writeCSV)
	    {
		LoadData.customerAppend(sbCustomer);
		LoadData.historyAppend(sbHistory);
	    }
	    else
	    {
		stmtCustomer.executeBatch();
		stmtCustomer.clearBatch();
		dbConn.commit();
		stmtHistory.executeBatch();
		stmtHistory.clearBatch();
		dbConn.commit();
	    }

	    /*
	     * For the ORDER rows the TPC-C specification demands that they
	     * are generated using a random permutation of all 3,000
	     * customers. To do that we set up an array with all C_IDs
	     * and then randomly shuffle it.
	     */
	    int randomCID[] = new int[3000];
	    for (int i = 0; i < 3000; i++)
		randomCID[i] = i + 1;
	    for (int i = 0; i < 3000; i++)
	    {
		int x = rnd.nextInt(0, 2999);
		int y = rnd.nextInt(0, 2999);
		int tmp = randomCID[x];
		randomCID[x] = randomCID[y];
		randomCID[y] = tmp;
	    }

	    for (int o_id = 1; o_id <= 3000; o_id++)
	    {
		int     o_ol_cnt = rnd.nextInt(5, 15);

			// commit district and history when 100 records
			if (o_id != 1 && (o_id - 1) % 100 == 0)
			{
				if (writeCSV)
				{
					LoadData.orderAppend(sbOrder);
					LoadData.orderLineAppend(sbOrderLine);
					LoadData.newOrderAppend(sbNewOrder);
				}
				else
				{
					stmtOrder.executeBatch();
					stmtOrder.clearBatch();
					dbConn.commit();

					stmtOrderLine.executeBatch();
					stmtOrderLine.clearBatch();
					dbConn.commit();

					stmtNewOrder.executeBatch();
					stmtNewOrder.clearBatch();
					dbConn.commit();
				}
			}

		if (writeCSV)
		{
		    fmtOrder.format("%d,%d,%d,%d,%s,%d,%d,%s\n",
			w_id,
			d_id,
			o_id,
			randomCID[o_id - 1],
			(o_id < 2101) ? rnd.nextInt(1, 10) : csvNull,
			o_ol_cnt,
			1,
			new java.sql.Timestamp(System.currentTimeMillis()).toString());
		}
		else
		{
			stmtOrder.setInt(1, w_id);
			stmtOrder.setInt(2, d_id);
			stmtOrder.setInt(3, o_id);
		    stmtOrder.setInt(4, randomCID[o_id - 1]);
			if (o_id < 2101)
				stmtOrder.setInt(5, rnd.nextInt(1, 10));
			else
				stmtOrder.setNull(5, java.sql.Types.INTEGER);
			stmtOrder.setInt(6, o_ol_cnt);
			stmtOrder.setInt(7, 1);
			stmtOrder.setTimestamp(8, new java.sql.Timestamp(System.currentTimeMillis()));

		    stmtOrder.addBatch();
		}

		/*
		 * Create the ORDER_LINE rows for this ORDER.
		 */
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++)
		{
		    long now = System.currentTimeMillis();

		    if (writeCSV)
		    {
			fmtOrderLine.format("%d,%d,%d,%d,%d,%s,%.2f,%d,%d,%s\n",
			    w_id,
			    d_id,
			    o_id,
			    ol_number,
			    rnd.nextInt(1, 100000),
			    (o_id < 2101) ? new java.sql.Timestamp(now).toString() : csvNull,
			    (o_id < 2101) ? 0.00 : ((double)rnd.nextLong(1, 999999)) / 100.0,
			    w_id,
			    5,
			    rnd.getAString(24, 24));
		    }
		    else
		    {
			stmtOrderLine.setInt(1, w_id);
			stmtOrderLine.setInt(2, d_id);
			stmtOrderLine.setInt(3, o_id);
			stmtOrderLine.setInt(4, ol_number);
			stmtOrderLine.setInt(5, rnd.nextInt(1, 100000));
			if (o_id < 2101)
				stmtOrderLine.setTimestamp(6, new java.sql.Timestamp(now));
			else
				stmtOrderLine.setNull(6, java.sql.Types.TIMESTAMP);
			if (o_id < 2101)
				stmtOrderLine.setDouble(7, 0.00);
			else
				stmtOrderLine.setDouble(7, ((double)rnd.nextLong(1, 999999)) / 100.0);
			stmtOrderLine.setInt(8, w_id);
			stmtOrderLine.setInt(9, 5);
			stmtOrderLine.setString(10, rnd.getAString(24, 24));

			stmtOrderLine.addBatch();
		    }
		}

		/*
		 * The last 900 ORDERs are not yet delieverd and have a
		 * row in NEW_ORDER.
		 */
		if (o_id >= 2101)
		{
		    if (writeCSV)
		    {
			fmtNewOrder.format("%d,%d,%d\n",
			    w_id,
			    d_id,
			    o_id);
		    }
		    else
		    {
			stmtNewOrder.setInt(1, w_id);
			stmtNewOrder.setInt(2, d_id);
			stmtNewOrder.setInt(3, o_id);

			stmtNewOrder.addBatch();
		    }
		}
	    }

	    if (writeCSV)
	    {
		LoadData.orderAppend(sbOrder);
		LoadData.orderLineAppend(sbOrderLine);
		LoadData.newOrderAppend(sbNewOrder);
	    }
	    else
	    {
		stmtOrder.executeBatch();
		stmtOrder.clearBatch();
		dbConn.commit();
		stmtOrderLine.executeBatch();
		stmtOrderLine.clearBatch();
		dbConn.commit();
		stmtNewOrder.executeBatch();
		stmtNewOrder.clearBatch();
		dbConn.commit();
	    }
	}

	if (!writeCSV)
	    dbConn.commit();
    } // End loadWarehouse()
}
