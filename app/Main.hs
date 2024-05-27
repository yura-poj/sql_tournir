-----------------------------------------------------------------------------
-- |
-- Module      :  Main
-- Copyright   :  (c) 2019-2021 Konstantin Pugachev
--                (c) 2019 Denis Miginsky
-- License     :  MIT
--
-- Maintainer  :  K.V.Pugachev@inp.nsk.su
-- Stability   :  experimental
-- Portability :  portable
--
-- The SQLHSExample module provides examples of using SQLHS/SQLHSSugar module.
--
-----------------------------------------------------------------------------

import SQLHSSugar
import DBReader

-- CATEGORY:     WARE,    CLASS
-- MANUFACTURER: BILL_ID, COMPANY
-- MATERIAL:     BILL_ID, WARE,   AMOUNT
-- PRODUCT:      BILL_ID, WARE,   AMOUNT, PRICE

main = readDB' defaultDBName >>= executeSomeQueries

executeSomeQueries :: (Named Table, Named Table, Named Table, Named Table) -> IO ()
executeSomeQueries (categories, manufacturers, materials, products) = do
  test "lecPlan1'" lecPlan1'
  test "lecPlan2" lecPlan2
  test "lecPlan3" lecPlan3
  test "lecPlan4" lecPlan4
  test "lecPlan4'" lecPlan4'
  
  where
    test msg p = do
      putStrLn $ "===== execute " ++ msg ++ " ====="
      -- putStrLn . debugTable $ p & enumerate
      printResult $ p & enumerate
    
    lecPlan1' =
      (categories // "c" `indexby` col "WARE" & flatten) `wher` col "CLASS" `eq` str "Mineral"
      `mjoin` (materials // "m" `indexby` col "WARE" & flatten) `on` "c.WARE" `jeq` "m.WARE"
      `hjoin` (products // "p" `indexby` col "BILL_ID") `on` col "m.BILL_ID"
      `orderby` ["p.WARE":asc]
      `select` ["p.WARE"]
      & distinct 
      --20570


   
    lecPlan2 = 
      (categories // "c" `indexby` col "WARE" & flatten) `wher` (col "CLASS" `eq` str "Stuff")
      `mjoin` (products // "p" `indexby` col "WARE" & flatten) `on` "c.WARE" `jeq` "p.WARE"
      `hjoin` (categories // "c2" `indexby` col "CLASS") `on` str "Mineral"
      `hjoin` (materials // "m" `indexby` col "BILL_ID") `on` col "p.BILL_ID"
      `wher` col "c2.WARE" `eq` col "m.WARE"
      `orderby` ["p.BILL_ID":asc]
      `select` ["p.BILL_ID", "p.WARE", "m.WARE"]
      & distinct 
      & limit 0 50

      --40825


  
    lecPlan3 = 
      (manufacturers // "m1" `hjoin` (materials // "mat1" `indexby` col "BILL_ID") `on` col "m1.BILL_ID")
      `hjoin` (products // "p1" `indexby` col "BILL_ID") `on` col "m1.BILL_ID"
      `hjoin` (manufacturers // "m2" `indexby` col "COMPANY") `on` col "m1.COMPANY"
      `hjoin` (materials // "mat2" `indexby` col "BILL_ID") `on` col "m2.BILL_ID"
      `wher` (col "mat2.WARE" `eq` col "p1.WARE")
      `hjoin` (products // "p2" `indexby` col "BILL_ID") `on` col "mat2.BILL_ID"
      `orderby` ["m1.COMPANY":asc]
      `select` ["m1.COMPANY"] 
      & distinct
      --148604

    lecPlan4 = 
      -- CATEGORY FILTER c.CLASS='Raw food'
      (categories // "c" `indexby` col "WARE" & flatten) `wher` col "CLASS" `eq` str "Raw food"
      -- -> MERGE_JOIN PRODUCT INDEX BY WARE ON c.WARE=p.WARE
      `mjoin` (products // "p" `indexby` col "WARE" & flatten) `on` "c.WARE" `jeq` "p.WARE"
      -- -> HASH_JOIN MANUFACTURER INDEX BY BILL_ID ON m.BILL_ID=p.BILL_ID
      `hjoin` (manufacturers // "m" `indexby` col "BILL_ID") `on` col "p.BILL_ID"
      -- -> MAP (p.WARE, m.COMPANY)
      `select` ["p.WARE", "m.COMPANY"]
      -- ->DISTINCT
      & distinct 
      -- -> TAKE 10
      & limit 0 10
  
    lecPlan4' = 
      -- CATEGORY FILTER c.CLASS='Raw food'
      (categories // "c" `indexby` col "WARE" & flatten) `wher` col "CLASS" `eq` str "Raw food"
      -- -> HASH_JOIN PRODUCT INDEX BY WARE ON c.WARE=p.WARE
      `hjoin` (products // "p" `indexby` col "WARE") `on` col "c.WARE"
      -- -> HASH_JOIN MANUFACTURER INDEX BY BILL_ID ON m.BILL_ID=p.BILL_ID
      `hjoin` (manufacturers // "m" `indexby` col "BILL_ID") `on` col "p.BILL_ID"
      -- -> MAP (p.WARE, m.COMPANY)
      `select` ["p.WARE", "m.COMPANY"]
      -- ->DISTINCT
      & distinct 
      -- -> TAKE 10
      & limit 0 10
