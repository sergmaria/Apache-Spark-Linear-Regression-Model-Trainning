{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "\n",
    "spark = (SparkSession\n",
    "        .builder\n",
    "        .appName(\"Spark Assignment\")\n",
    "        .getOrCreate())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [],
   "source": [
    "file = \"C:/Users/maria/Downloads/Spark-Assignment/671009038_T_ONTIME_REPORTING.csv\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [],
   "source": [
    "flights = (spark.read.format(\"csv\")\n",
    "       .option(\"inferSchema\", \"true\")\n",
    "       .option(\"header\", \"true\")\n",
    "       .load(file))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+--------+-------+------+--------------------+----+------------------+--------+---------+--------+---------+---------+-----------------+--------+-------------+-------------+---------+--------------+-------------------+----+\n",
      "|   FL_DATE|TAIL_NUM|CARRIER|ORIGIN|    ORIGIN_CITY_NAME|DEST|    DEST_CITY_NAME|DEP_TIME|DEP_DELAY|ARR_TIME|ARR_DELAY|CANCELLED|CANCELLATION_CODE|DIVERTED|CARRIER_DELAY|WEATHER_DELAY|NAS_DELAY|SECURITY_DELAY|LATE_AIRCRAFT_DELAY|_c19|\n",
      "+----------+--------+-------+------+--------------------+----+------------------+--------+---------+--------+---------+---------+-----------------+--------+-------------+-------------+---------+--------------+-------------------+----+\n",
      "|2019-01-01|  N8974C|     9E|   AVL|       Asheville, NC| ATL|       Atlanta, GA|    1658|     -7.0|    1758|    -22.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N922XJ|     9E|   JFK|        New York, NY| RDU|Raleigh/Durham, NC|    1122|     -8.0|    1255|    -29.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N326PQ|     9E|   CLE|       Cleveland, OH| DTW|       Detroit, MI|    1334|     -7.0|    1417|    -31.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N135EV|     9E|   BHM|      Birmingham, AL| ATL|       Atlanta, GA|    1059|     -1.0|    1255|     -8.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N914XJ|     9E|   GTF|     Great Falls, MT| MSP|   Minneapolis, MN|    1057|     -3.0|    1418|    -17.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N924EV|     9E|   GRB|       Green Bay, WI| DTW|       Detroit, MI|     855|      0.0|    1125|     10.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N195PQ|     9E|   AGS|         Augusta, GA| ATL|       Atlanta, GA|     800|     -5.0|     900|    -16.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N319PQ|     9E|   CLT|       Charlotte, NC| JFK|      New York, NY|    1350|    -10.0|    1534|    -29.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N933XJ|     9E|   MEM|         Memphis, TN| MSP|   Minneapolis, MN|    1441|     -4.0|    1641|    -18.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N933XJ|     9E|   MSP|     Minneapolis, MN| MEM|       Memphis, TN|     847|     -4.0|    1114|     -6.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N8886A|     9E|   ATL|         Atlanta, GA| AEX|    Alexandria, LA|    1856|     -5.0|    1931|    -20.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N302PQ|     9E|   AGS|         Augusta, GA| ATL|       Atlanta, GA|    1427|     -9.0|    1540|     -4.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N302PQ|     9E|   ATL|         Atlanta, GA| AGS|       Augusta, GA|    1259|     -6.0|    1343|    -18.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N272PQ|     9E|   CVG|      Cincinnati, OH| LGA|      New York, NY|     834|     -6.0|    1022|    -18.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N292PQ|     9E|   RDU|  Raleigh/Durham, NC| MCO|       Orlando, FL|    1324|     -1.0|    1504|    -14.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N980EV|     9E|   GSO|Greensboro/High P...| LGA|      New York, NY|    1155|     -5.0|    1340|      3.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N326PQ|     9E|   BIS| Bismarck/Mandan, ND| MSP|   Minneapolis, MN|     809|    124.0|     933|    109.0|      0.0|             null|     0.0|        109.0|          0.0|      0.0|           0.0|                0.0|null|\n",
      "|2019-01-01|  N135EV|     9E|   ATL|         Atlanta, GA| AVL|     Asheville, NC|    1531|     -4.0|    1625|    -10.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N8976E|     9E|   LGA|        New York, NY| BTV|    Burlington, VT|    1756|     -4.0|    1913|     -7.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N925XJ|     9E|   PIT|      Pittsburgh, PA| JFK|      New York, NY|    1124|     -1.0|    1249|     -4.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "+----------+--------+-------+------+--------------------+----+------------------+--------+---------+--------+---------+---------+-----------------+--------+-------------+-------------+---------+--------------+-------------------+----+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "flights.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [],
   "source": [
    "flights.createOrReplaceTempView(\"flights\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Taking care of the outliers"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------+-----+\n",
      "|ORIGIN|count|\n",
      "+------+-----+\n",
      "|   BGM|  958|\n",
      "|   PSE|  825|\n",
      "|   INL|  648|\n",
      "|   DLG|   82|\n",
      "|   MSY|58164|\n",
      "|   PPG|  120|\n",
      "|   GEG|12034|\n",
      "|   DRT|  723|\n",
      "|   SNA|40754|\n",
      "|   BUR|31905|\n",
      "|   GTF| 1797|\n",
      "|   GRB| 5169|\n",
      "|   IDA| 2120|\n",
      "|   GRR|19197|\n",
      "|   LWB|  665|\n",
      "|   PVU|  731|\n",
      "|   JLN| 1386|\n",
      "|   EUG| 5217|\n",
      "|   PSG|  724|\n",
      "|   ATY|  523|\n",
      "+------+-----+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "flights_groupeby_origin = flights.groupby('ORIGIN').count()\n",
    "\n",
    "flights_groupeby_origin.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------+-----+\n",
      "|ORIGIN|count|\n",
      "+------+-----+\n",
      "|   AKN|   61|\n",
      "|   PGV|   80|\n",
      "|   DLG|   82|\n",
      "|   GST|   82|\n",
      "|   HYA|   83|\n",
      "|   OWB|  102|\n",
      "|   ADK|  104|\n",
      "|   OGD|  105|\n",
      "|   PPG|  120|\n",
      "|   STC|  136|\n",
      "|   BFM|  158|\n",
      "|   HGR|  183|\n",
      "|   SMX|  193|\n",
      "|   XWA|  207|\n",
      "|   BKG|  231|\n",
      "|   GUC|  240|\n",
      "|   ART|  245|\n",
      "|   WYS|  264|\n",
      "|   OTH|  383|\n",
      "|   PSM|  425|\n",
      "+------+-----+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "flights_groupeby_origin.orderBy('count', ascending=True).show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[82.0]"
      ]
     },
     "execution_count": 10,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "outliers_origin = flights_groupeby_origin.approxQuantile(\"count\", [0.01], 0)\n",
    "outliers_origin"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------+-----+\n",
      "|ORIGIN|count|\n",
      "+------+-----+\n",
      "|   BGM|  958|\n",
      "|   PSE|  825|\n",
      "|   INL|  648|\n",
      "|   MSY|58164|\n",
      "|   PPG|  120|\n",
      "|   GEG|12034|\n",
      "|   DRT|  723|\n",
      "|   SNA|40754|\n",
      "|   BUR|31905|\n",
      "|   GTF| 1797|\n",
      "|   GRB| 5169|\n",
      "|   IDA| 2120|\n",
      "|   GRR|19197|\n",
      "|   LWB|  665|\n",
      "|   PVU|  731|\n",
      "|   JLN| 1386|\n",
      "|   EUG| 5217|\n",
      "|   PSG|  724|\n",
      "|   ATY|  523|\n",
      "|   GSO|14634|\n",
      "+------+-----+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "flights_groupeby_origin = flights_groupeby_origin.filter(flights_groupeby_origin['count']>82).show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------+-------+\n",
      "|CARRIER|  count|\n",
      "+-------+-------+\n",
      "|     UA| 625910|\n",
      "|     NK| 204845|\n",
      "|     AA| 946776|\n",
      "|     EV| 134683|\n",
      "|     B6| 297411|\n",
      "|     DL| 991986|\n",
      "|     OO| 836445|\n",
      "|     F9| 135543|\n",
      "|     YV| 227888|\n",
      "|     MQ| 327007|\n",
      "|     OH| 289304|\n",
      "|     HA|  83891|\n",
      "|     G4| 105305|\n",
      "|     YX| 329149|\n",
      "|     AS| 264816|\n",
      "|     WN|1363946|\n",
      "|     9E| 257132|\n",
      "+-------+-------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "flights_groupeby_carrier = flights.groupby('CARRIER').count()\n",
    "\n",
    "flights_groupeby_carrier.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------+-------+\n",
      "|CARRIER|  count|\n",
      "+-------+-------+\n",
      "|     HA|  83891|\n",
      "|     G4| 105305|\n",
      "|     EV| 134683|\n",
      "|     F9| 135543|\n",
      "|     NK| 204845|\n",
      "|     YV| 227888|\n",
      "|     9E| 257132|\n",
      "|     AS| 264816|\n",
      "|     OH| 289304|\n",
      "|     B6| 297411|\n",
      "|     MQ| 327007|\n",
      "|     YX| 329149|\n",
      "|     UA| 625910|\n",
      "|     OO| 836445|\n",
      "|     AA| 946776|\n",
      "|     DL| 991986|\n",
      "|     WN|1363946|\n",
      "+-------+-------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "flights_groupeby_carrier.orderBy('count', ascending=True).show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[83891.0]"
      ]
     },
     "execution_count": 14,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "outliers_carrier = flights_groupeby_carrier.approxQuantile(\"count\", [0.01], 0)\n",
    "outliers_carrier"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------+-------+\n",
      "|CARRIER|  count|\n",
      "+-------+-------+\n",
      "|     UA| 625910|\n",
      "|     NK| 204845|\n",
      "|     AA| 946776|\n",
      "|     EV| 134683|\n",
      "|     B6| 297411|\n",
      "|     DL| 991986|\n",
      "|     OO| 836445|\n",
      "|     F9| 135543|\n",
      "|     YV| 227888|\n",
      "|     MQ| 327007|\n",
      "|     OH| 289304|\n",
      "|     G4| 105305|\n",
      "|     YX| 329149|\n",
      "|     AS| 264816|\n",
      "|     WN|1363946|\n",
      "|     9E| 257132|\n",
      "+-------+-------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "flights_groupeby_carrier = flights_groupeby_carrier.filter(flights_groupeby_carrier['count']>83891).show()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "So that seems that the airports that have 82 flights or less must be excluded from the dataset"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+----------+--------+-------+------+--------------------+----+------------------+--------+---------+--------+---------+---------+-----------------+--------+-------------+-------------+---------+--------------+-------------------+----+\n",
      "|   FL_DATE|TAIL_NUM|CARRIER|ORIGIN|    ORIGIN_CITY_NAME|DEST|    DEST_CITY_NAME|DEP_TIME|DEP_DELAY|ARR_TIME|ARR_DELAY|CANCELLED|CANCELLATION_CODE|DIVERTED|CARRIER_DELAY|WEATHER_DELAY|NAS_DELAY|SECURITY_DELAY|LATE_AIRCRAFT_DELAY|_c19|\n",
      "+----------+--------+-------+------+--------------------+----+------------------+--------+---------+--------+---------+---------+-----------------+--------+-------------+-------------+---------+--------------+-------------------+----+\n",
      "|2019-01-01|  N8974C|     9E|   AVL|       Asheville, NC| ATL|       Atlanta, GA|    1658|     -7.0|    1758|    -22.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N922XJ|     9E|   JFK|        New York, NY| RDU|Raleigh/Durham, NC|    1122|     -8.0|    1255|    -29.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N326PQ|     9E|   CLE|       Cleveland, OH| DTW|       Detroit, MI|    1334|     -7.0|    1417|    -31.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N135EV|     9E|   BHM|      Birmingham, AL| ATL|       Atlanta, GA|    1059|     -1.0|    1255|     -8.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N914XJ|     9E|   GTF|     Great Falls, MT| MSP|   Minneapolis, MN|    1057|     -3.0|    1418|    -17.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N924EV|     9E|   GRB|       Green Bay, WI| DTW|       Detroit, MI|     855|      0.0|    1125|     10.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N195PQ|     9E|   AGS|         Augusta, GA| ATL|       Atlanta, GA|     800|     -5.0|     900|    -16.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N319PQ|     9E|   CLT|       Charlotte, NC| JFK|      New York, NY|    1350|    -10.0|    1534|    -29.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N933XJ|     9E|   MEM|         Memphis, TN| MSP|   Minneapolis, MN|    1441|     -4.0|    1641|    -18.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N933XJ|     9E|   MSP|     Minneapolis, MN| MEM|       Memphis, TN|     847|     -4.0|    1114|     -6.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N8886A|     9E|   ATL|         Atlanta, GA| AEX|    Alexandria, LA|    1856|     -5.0|    1931|    -20.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N302PQ|     9E|   AGS|         Augusta, GA| ATL|       Atlanta, GA|    1427|     -9.0|    1540|     -4.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N302PQ|     9E|   ATL|         Atlanta, GA| AGS|       Augusta, GA|    1259|     -6.0|    1343|    -18.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N272PQ|     9E|   CVG|      Cincinnati, OH| LGA|      New York, NY|     834|     -6.0|    1022|    -18.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N292PQ|     9E|   RDU|  Raleigh/Durham, NC| MCO|       Orlando, FL|    1324|     -1.0|    1504|    -14.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N980EV|     9E|   GSO|Greensboro/High P...| LGA|      New York, NY|    1155|     -5.0|    1340|      3.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N326PQ|     9E|   BIS| Bismarck/Mandan, ND| MSP|   Minneapolis, MN|     809|    124.0|     933|    109.0|      0.0|             null|     0.0|        109.0|          0.0|      0.0|           0.0|                0.0|null|\n",
      "|2019-01-01|  N135EV|     9E|   ATL|         Atlanta, GA| AVL|     Asheville, NC|    1531|     -4.0|    1625|    -10.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N8976E|     9E|   LGA|        New York, NY| BTV|    Burlington, VT|    1756|     -4.0|    1913|     -7.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "|2019-01-01|  N925XJ|     9E|   PIT|      Pittsburgh, PA| JFK|      New York, NY|    1124|     -1.0|    1249|     -4.0|      0.0|             null|     0.0|         null|         null|     null|          null|               null|null|\n",
      "+----------+--------+-------+------+--------------------+----+------------------+--------+---------+--------+---------+---------+-----------------+--------+-------------+-------------+---------+--------------+-------------------+----+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "flights = flights.filter(\"`ORIGIN` != 'AKN'\")\n",
    "flights = flights.filter(\"`ORIGIN` != 'PGV'\")\n",
    "flights = flights.filter(\"`ORIGIN` != 'DLG'\")\n",
    "flights = flights.filter(\"`ORIGIN` != 'GST'\")\n",
    "flights = flights.filter(\"`CARRIER` != 'HA'\")\n",
    "\n",
    "flights.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------------------+------+\n",
      "|    avg(DEP_DELAY)|ORIGIN|\n",
      "+------------------+------+\n",
      "| 33.78393351800554|   OTH|\n",
      "|32.604878048780485|   XWA|\n",
      "| 30.97339246119734|   MMH|\n",
      "|29.349397590361445|   HYA|\n",
      "|28.883597883597883|   MEI|\n",
      "|28.147540983606557|   ACK|\n",
      "| 26.46260017809439|   EGE|\n",
      "|26.192251461988302|   MQT|\n",
      "|25.175824175824175|   HGR|\n",
      "| 24.05037037037037|   CMX|\n",
      "|23.682297154899896|   ACV|\n",
      "|23.590975254730715|   SHD|\n",
      "| 23.40711462450593|   OGS|\n",
      "|23.298076923076923|   OGD|\n",
      "|23.244550858652577|   ASE|\n",
      "|22.853982300884955|   SLN|\n",
      "|22.178612716763006|   SWF|\n",
      "|21.483647175421208|   BLV|\n",
      "|21.414165666266506|   CKB|\n",
      "|21.315384615384616|   STC|\n",
      "+------------------+------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "avg_airport_delay = spark.sql(\"\"\"\n",
    "SELECT avg(DEP_DELAY), ORIGIN \n",
    "FROM flights\n",
    "GROUP BY ORIGIN\n",
    "ORDER BY avg(DEP_DELAY) DESC\n",
    "\"\"\")\n",
    "avg_airport_delay.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "metadata": {},
   "outputs": [],
   "source": [
    "avg_airport_delay.toPandas().to_csv('task2-ap-avg.csv')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------+---------------------+\n",
      "|ORIGIN|MEDIAN DELAYS AIRPORT|\n",
      "+------+---------------------+\n",
      "|   BGM|                 -4.0|\n",
      "|   INL|                 -8.0|\n",
      "|   PSE|                 -6.0|\n",
      "|   MSY|                 -1.0|\n",
      "|   DRT|                 -5.0|\n",
      "|   GEG|                 -3.0|\n",
      "|   BUR|                 -2.0|\n",
      "|   SNA|                 -2.0|\n",
      "|   GRB|                 -5.0|\n",
      "|   GTF|                 -4.0|\n",
      "|   IDA|                 -4.0|\n",
      "|   GRR|                 -3.0|\n",
      "|   LWB|                 -8.0|\n",
      "|   JLN|                 -4.0|\n",
      "|   PVU|                 -1.0|\n",
      "|   EUG|                 -4.0|\n",
      "|   PSG|                 -8.0|\n",
      "|   ATY|                 -9.0|\n",
      "|   GSO|                 -4.0|\n",
      "|   MYR|                 -4.0|\n",
      "+------+---------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "import pyspark.sql.functions as F\n",
    "\n",
    "median_airport = flights \\\n",
    "    .groupby('ORIGIN') \\\n",
    "    .agg(F.expr('percentile(DEP_DELAY, array(0.5))')[0].alias('MEDIAN DELAYS AIRPORT'))\n",
    "    \n",
    "median_airport.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "metadata": {},
   "outputs": [],
   "source": [
    "median_airport = median_airport.orderBy('MEDIAN DELAYS AIRPORT', ascending=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "metadata": {},
   "outputs": [],
   "source": [
    "median_airport.createOrReplaceTempView(\"median_airport\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "metadata": {},
   "outputs": [],
   "source": [
    "median_airport.toPandas().to_csv('task2-ap-med.csv')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------------------+-------+\n",
      "|    avg(DEP_DELAY)|CARRIER|\n",
      "+------------------+-------+\n",
      "|17.745473360997547|     B6|\n",
      "| 17.21400657286479|     EV|\n",
      "|14.577009288343467|     F9|\n",
      "| 13.80316319064953|     YV|\n",
      "|13.004563709088917|     UA|\n",
      "|12.564053392669365|     OO|\n",
      "|12.114915337571487|     AA|\n",
      "|10.940950394756443|     NK|\n",
      "|10.704733524922498|     OH|\n",
      "|10.245764586889184|     9E|\n",
      "|10.178762481230244|     WN|\n",
      "|10.122909563240786|     G4|\n",
      "| 9.272981408689438|     MQ|\n",
      "| 8.544063599958992|     YX|\n",
      "| 8.155754169633253|     DL|\n",
      "|5.0346367947220125|     AS|\n",
      "|1.2963997517070143|     HA|\n",
      "+------------------+-------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "avg_carrier_delay = spark.sql(\"\"\"\n",
    "SELECT avg(DEP_DELAY), CARRIER \n",
    "FROM flights\n",
    "GROUP BY CARRIER\n",
    "ORDER BY avg(DEP_DELAY) DESC\n",
    "\"\"\")\n",
    "avg_carrier_delay.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "metadata": {},
   "outputs": [],
   "source": [
    "avg_carrier_delay.toPandas().to_csv('task2-aw-avg.csv')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+-------+---------------------+\n",
      "|CARRIER|MEDIAN CARRIER DELAYS|\n",
      "+-------+---------------------+\n",
      "|     UA|                 -3.0|\n",
      "|     NK|                 -3.0|\n",
      "|     AA|                 -2.0|\n",
      "|     EV|                 -4.0|\n",
      "|     B6|                 -3.0|\n",
      "|     DL|                 -2.0|\n",
      "|     OO|                 -3.0|\n",
      "|     F9|                 -3.0|\n",
      "|     YV|                 -3.0|\n",
      "|     MQ|                 -3.0|\n",
      "|     OH|                 -3.0|\n",
      "|     G4|                 -3.0|\n",
      "|     YX|                 -4.0|\n",
      "|     AS|                 -4.0|\n",
      "|     WN|                  0.0|\n",
      "|     9E|                 -4.0|\n",
      "+-------+---------------------+\n",
      "\n"
     ]
    }
   ],
   "source": [
    "import pyspark.sql.functions as F\n",
    "\n",
    "median_carrier = flights \\\n",
    "    .groupby('CARRIER') \\\n",
    "    .agg(F.expr('percentile(DEP_DELAY, array(0.5))')[0].alias('MEDIAN CARRIER DELAYS'))\n",
    "\n",
    "median_carrier.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "metadata": {},
   "outputs": [],
   "source": [
    "median_carrier = median_carrier.orderBy('MEDIAN CARRIER DELAYS', ascending=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "metadata": {},
   "outputs": [],
   "source": [
    "median_carrier.toPandas().to_csv('task2-aw-med.csv')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
