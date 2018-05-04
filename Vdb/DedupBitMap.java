package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;
import Utils.*;


/**
 * This bitmap decides which data block becomes a Unique data block or a
 * duplicate.
 * There is one bitmap per SD/FSD.
 *
 * Technically is should be OK to have just ONE bitmap for the whole Dedup
 * instace,  works....
 */
public class DedupBitMap
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private BitSet[] bitsets = null;
  private long     bits_needed;
  private long     bits_allocated;

  /* This must be a power of two: */
  private final static long  MAX_BITS  = 1024 * 1024 * 1024;
  private final static int   BIT_SHIFT = 30;
  private final static long  BIT_AND   = (1 << BIT_SHIFT) - 1;


  private static HashMap <String, DedupBitMap> unique_maps   = new HashMap(8);
  private static HashMap <String, DedupBitMap> flipflop_maps = new HashMap(8);

  public static void addUniqueBitmap(DedupBitMap bmap, String name)
  {
    if (unique_maps.put(name, bmap) != null)
      common.failure("Trying to create a second DedupBitMap for %s" , name);
  }

  public static DedupBitMap findUniqueBitmap(String name)
  {
    //common.ptod("name: " + name);
    //common.ptod("unique_maps: " + unique_maps.size());
    //for (String kname : unique_maps.keySet())
    //  common.ptod("kname: " + kname);

    return unique_maps.get(name);
  }

  /**
   * Create a bit map to help us decide which block is duplicate versus unique.
   */
  public DedupBitMap createMapForUniques(Dedup dedup, long bytes, String lun_or_anchor)
  {
    bits_needed = (bytes / dedup.getDedupUnit());

    /* One extra bit needed due to dedupunit straddling: */
    bits_allocated = bits_needed + 1;

    /* Accommodate multiple bitsets so that we can handle more than 2**31 entries: */
    int  sets = (int) ((bits_allocated + MAX_BITS - 1) / MAX_BITS);

    bitsets = new BitSet[ sets ];
    for (int i = 0; i < sets; i++)
    {
      bitsets [ i ]  = new BitSet( (int) Math.min(bits_allocated - i * MAX_BITS, MAX_BITS) );
      //common.ptod("bitsets [ i ]: " + bitsets [ i ].size());
    }


    /* Initialize table: */
    double adjusted_pct = dedup.getAdjustedPct();
    long   bits_to_set  = (long) (bits_needed * adjusted_pct / 100.);

    common.ptod("Creating bitmap for unique blocks for %s. %d bitmaps for a total of %,d bits, identifying %,d unique blocks",
                lun_or_anchor, sets, bits_needed, bits_to_set);

    if (bits_to_set > bits_needed)
      common.failure("Oops, asking for %,d unique blocks but have only %,d blocks to choose from",
                     bits_to_set, bits_needed);

    /* If this dedupratio=1 then don't bother setting the bits.                    */
    /* We still keep the bitmap so that we don't have differences in memory usage. */
    if (dedup.getDedupRatio() == 1)
      return this;

    long uniques     = 0;
    long collissions = 0;
    long max_block   = 0;
    Random rand = new Random(lun_or_anchor.hashCode());
    Elapsed elapsed = new Elapsed("createMapForUniques", 100*1000*1000);

    for (long i = 0; uniques < bits_to_set; i++)
    {
      /* See this note from Random.nextLong():                              */
      /* "Because class {@code Random} uses a seed with only 48 bits,       */
      /*  this algorithm will not return all possible {@code long} values." */
      /* This tells me that I can expect 48 bits, not 64? Good enough.      */
      /* I also are using only 48 bites in journaling.                      */
      /* 48 bits = 281,474,976,710,655                                      */
      /* This will handle 240 PB of 512 byte blocks.                        */
      long block = Math.abs(rand.nextLong() % bits_needed);

      /* If the randomizer selects block0 it is possible that with smaller */
      /* files there won't ever be any references to a dedupset.           */
      /* This causes confusion, so I forcibly prevent this scenario.       */
      /* I bet though that there can be other scenarios also, which I      */
      /* have not figured out yet.                                         */
      /* Decided to just let it be.                                        */
      //if (block == 0)
      //  continue;


      // just for debugging
      max_block = Math.max(max_block, block);

      /* The first 'n' hot blocks of course may not be unique, so we adjust: */
      if (block < dedup.hot_dedup_blocks)
        continue;

      /* Long bitmaps take a long time to initialize. Mostly CPU+memory cache */
      /* 1.5 billion random bits: 450 seconds.                                */
      /* By doing a batch of 32 in one swoop this went down to 35 seconds.    */
      /* This means we have groups of 32 'unique' blocks in a row, but for    */
      /* what I want that's just fine.                                        */
      for (int j = 0; j < 32 && uniques < bits_to_set; j++)
      {
        long set_block = block + j;

        /* If we have a random bit at the END of the map, there is no room for more: */
        if (set_block >= bits_needed)
          break;

        if (getBit(set_block))
        {
          collissions++;
          continue;
        }

        setBit(set_block, true);
        uniques++;

        //common.ptod("bitsets [ i ]: " + bitsets [ 0 ].size());

        //if (uniques % (10*1000*1000) == 0)
        //  common.ptod("unique count %,16d bit set for block: %,16d collissions: %,16d max: %,16d",
        //              uniques, block, collissions, max_block);
      }
    }

    elapsed.end(5);

    return this;
  }

  /**
   * Create a bit map to handle dedup flipflop mechanism.
   * Map is all zeroes at the start.
   */
  public DedupBitMap createMapForFlipFlop(Dedup dedup, long bits, String lun_or_anchor)
  {
    /* One extra bit needed due to dedupunit straddling: */
    bits_needed    = bits;
    bits_allocated = bits_needed + 1;

    /* Accommodate multiple bitsets so that we can handle more than 2**31 entries: */
    int  sets = (int) ((bits_allocated + MAX_BITS - 1) / MAX_BITS);

    bitsets = new BitSet[ sets ];
    for (int i = 0; i < sets; i++)
    {
      bitsets [ i ]  = new BitSet( (int) Math.min(bits_allocated - i * MAX_BITS, MAX_BITS) );
      //common.ptod("bitsets [ i ]: " + bitsets [ i ].size());
    }

    common.ptod("Created flipflop bitmap for %s. %d bitmaps for a total of %,d bits.",
                lun_or_anchor, sets, bits_needed);

    return this;
  }

  public void setBit(long bit, boolean bool)
  {
    int set       = (int) (bit >> BIT_SHIFT);
    int remainder = (int) (bit  & BIT_AND);
    bitsets[ set ].set(remainder, bool);
  }
  public boolean getBit(long bit)
  {
    int set       = (int) (bit >> BIT_SHIFT);
    int remainder = (int) (bit  & BIT_AND);
    return bitsets[ set ].get( remainder );
  }

  public boolean isUnique(long bit)
  {
    //if (bit >= bits_needed)
    if (bit >= bits_allocated)
      common.failure("DedupBitMap.isUnique(): requested index too high: %d/%d",
                     bits_needed, bit);
    return getBit(bit);
  }
  public boolean isDuplicate(long bit)
  {
    if (bit >= bits_needed)
      common.failure("DedupBitMap.isDuplicate(): requested index too high: %d/%d",
                     bits_needed, bit);
    return !getBit(bit);
  }

  public static long blockHash(long block)
  {
    //  int index = (int) (block &0x7f);
    //  int hash  = vd_polynomial_coefficients[index];
    //  return block * hash;

    //  long result = 17;
    //
    //  /* From Effective Java, page 38. */
    //  long lbax = (long) (lba    ^ (lba    >>> 16));
    //
    //  result = 37 * result + lbax;

    //  return result;

    return(int)(block ^ (37 * block >>> 32));
  }



  static int[] vd_polynomial_coefficients =
  {
    0x8AD8A4E8, 0x8B026ED8, 0x8B386301, 0x8BB55E2B, 0x8BB586D1, 0x8BFEC841,
    0x8C61EFE0, 0x8C9D4855, 0x8CEBE0DD, 0x8D058F13, 0x8DACD66D, 0x8E3CD7F9,
    0x8EAB9F5D, 0x8EC08385, 0x8EFDD7B0, 0x8F2B5568, 0x8F42C6DA, 0x8FB3E662,
    0x915E5A0F, 0x917CCCC9, 0x9260E006, 0x92BDA6E9, 0x92FDE296, 0x934CD0E4,
    0x934D6A55, 0x934DD265, 0x935466AE, 0x93E371DF, 0x9466D0D8, 0x94CB06B4,
    0x957B71ED, 0x95F882A7, 0x9635D44E, 0x963D360E, 0x970AB3F3, 0x97390A40,
    0x9752F0E2, 0x98313201, 0x98332229, 0x98AF1149, 0x99238C6A, 0x996A24BE,
    0x99D574CB, 0x9AF79122, 0x9B7ED838, 0x9BDA4C13, 0x9D53683F, 0x9D5FF2F9,
    0x9DF65A38, 0x9E26DFA2, 0x9E7EAF6B, 0x9EBE160C, 0x9F75CA73, 0x9F868146,
    0xA079BA6C, 0xA0B3BD68, 0xA1831C3D, 0xA1866FDB, 0xA1AB06F5, 0xA1B23118,
    0xA2283C26, 0xA3778316, 0xA3C0D8DB, 0xA410F83D, 0xA43DD058, 0xA442C7CC,
    0xA4720575, 0xA526DAC3, 0xA5E7E8BB, 0xA688D850, 0xA6D641E5, 0xA719AC4E,
    0xA7A3B1F1, 0xA84A1B21, 0xA92881CC, 0xA92DF947, 0xA95729FC, 0xAA9D6EB7,
    0xABB6C8C4, 0xABBB6D87, 0xAC098E27, 0xAC63DD72, 0xAC87D325, 0xACB80AB4,
    0xACF6AC8B, 0xAD412A87, 0xAD5450E8, 0xAED0E2D1, 0xB0768827, 0xB20E4FC0,
    0xB2626739, 0xB26F10C4, 0xB2B2D433, 0xB2CDFD03, 0xB383A02E, 0xB3BBD553,
    0xB3CF081E, 0xB3F20861, 0xB3F45D54, 0xB4D08B5E, 0xB4FCF7F4, 0xB539E491,
    0xB5F3EAAC, 0xB5F8D6F7, 0xB61CFE8F, 0xB6BAB103, 0xB73E25DD, 0xB7736D5A,
    0xB7D95002, 0xB8BF6B3C, 0xB95E33D2, 0xBA6511DC, 0xBA6C6141, 0xBB0D522B,
    0xBB190FBC, 0xBB553C48, 0xBBF10F34, 0xBBF29A64, 0xBC77028B, 0xBD7A6D3C,
    0xBF88A0C1, 0xBF962F30, 0xC01D184B, 0xC0FA97A7, 0xC10432E6, 0xC144D566,
    0xC14E6718, 0xC1A0B464
  };



  public static void main(String[] args)
  {
    long LOOP = 50;
    int flips = 0;
    int flops = 0;
    for (int i = 0; i < LOOP; i++)
    {
      long block = i;
      long hash  = blockHash(block);
      long which = hash &1;
      common.ptod("which: %08x %08x %d", block, hash, which);
      if (which == 1)
        flips++;
      else
        flops++;
    }

    double flip_pct = flips * 100. / LOOP;
    common.ptod("flips: " + flips);
    common.ptod("flops: " + flops);
    common.ptod("flip_pct: %.3f", flip_pct);
  }
}

