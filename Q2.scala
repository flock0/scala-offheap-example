package ch.epfl.data
package dblab.legobase



import scala.collection.mutable.Set
import scala.collection.mutable.HashMap
import scala.collection.mutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import storagemanager.K2DBScanner
import storagemanager.Loader
import queryengine.GenericEngine
import sc.pardis.shallow.OptimalString
import sc.pardis.shallow.scalalib.collection.Cont

class MultiMap[T, S] extends HashMap[T, Set[S]] with scala.collection.mutable.MultiMap[T, S]

object OrderingFactory {
  def apply[T](fun: (T, T) => Int): Ordering[T] = new Ordering[T] {
    def compare(o1: T, o2: T) = fun(o1, o2)
  }
}


case class REGIONRecord(val R_REGIONKEY: Int, val R_NAME: OptimalString)
case class WindowRecord_Int_DynamicCompositeRecord_REGIONRecord_DynamicCompositeRecord_PARTRecord_DynamicCompositeRecord_NATIONRecord_DynamicCompositeRecord_SUPPLIERRecord_PARTSUPPRecord(val wnd: REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord)
case class NATIONRecord(val N_NATIONKEY: Int, val N_NAME: OptimalString, val N_REGIONKEY: Int)
case class REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord(val R_REGIONKEY: Int, val R_NAME: OptimalString, val P_PARTKEY: Int, val P_MFGR: OptimalString, val P_TYPE: OptimalString, val P_SIZE: Int, val N_NATIONKEY: Int, val N_NAME: OptimalString, val N_REGIONKEY: Int, val S_SUPPKEY: Int, val S_NAME: OptimalString, val S_ADDRESS: OptimalString, val S_NATIONKEY: Int, val S_PHONE: OptimalString, val S_ACCTBAL: Double, val S_COMMENT: OptimalString, val PS_PARTKEY: Int, val PS_SUPPKEY: Int, val PS_SUPPLYCOST: Double)
case class PARTRecord(val P_PARTKEY: Int, val P_MFGR: OptimalString, val P_TYPE: OptimalString, val P_SIZE: Int)
case class SUPPLIERRecord(val S_SUPPKEY: Int, val S_NAME: OptimalString, val S_ADDRESS: OptimalString, val S_NATIONKEY: Int, val S_PHONE: OptimalString, val S_ACCTBAL: Double, val S_COMMENT: OptimalString)
case class Set_REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord(var maxSize: Int, val array: Array[REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord])
case class PARTSUPPRecord(val PS_PARTKEY: Int, val PS_SUPPKEY: Int, val PS_SUPPLYCOST: Double)
object Q2 extends LegoRunner {
  def executeQuery(query: String, sf: Double, schema: ch.epfl.data.dblab.legobase.schema.Schema): Unit = main()
  def main(args: Array[String]) {
    run(args)
  }
  def main() = 
  {
    val x1 = Loader.fileLineCount("/mnt/ramdisk/sf1/part.tbl")
    val x2 = new Array[PARTRecord](x1)
    val x3 = new K2DBScanner("/mnt/ramdisk/sf1/part.tbl")
    var x4: Int = 0
    val x64 = while({
      val x5 = x4
      val x6 = x5.<(x1)
      val x8 = x6.&&({
        val x7 = x3.hasNext()
        x7
      })
      x8
    })
    {
      val x9 = x3.next_int()
      val x11 = new Array[Byte](56)
      val x12 = x3.next(x11)
      val x15 = { x13: Byte => {
          val x14 = x13.!=(0)
          x14
        }
      }
      val x16 = x11.filter(x15)
      val x17 = new OptimalString(x16)
      val x19 = new Array[Byte](26)
      val x20 = x3.next(x19)
      val x23 = { x21: Byte => {
          val x22 = x21.!=(0)
          x22
        }
      }
      val x24 = x19.filter(x23)
      val x25 = new OptimalString(x24)
      val x27 = new Array[Byte](11)
      val x28 = x3.next(x27)
      val x31 = { x29: Byte => {
          val x30 = x29.!=(0)
          x30
        }
      }
      val x32 = x27.filter(x31)
      val x33 = new OptimalString(x32)
      val x34 = new Array[Byte](26)
      val x35 = x3.next(x34)
      val x38 = { x36: Byte => {
          val x37 = x36.!=(0)
          x37
        }
      }
      val x39 = x34.filter(x38)
      val x40 = new OptimalString(x39)
      val x41 = x3.next_int()
      val x42 = new Array[Byte](11)
      val x43 = x3.next(x42)
      val x46 = { x44: Byte => {
          val x45 = x44.!=(0)
          x45
        }
      }
      val x47 = x42.filter(x46)
      val x48 = new OptimalString(x47)
      val x49 = x3.next_double()
      val x51 = new Array[Byte](24)
      val x52 = x3.next(x51)
      val x55 = { x53: Byte => {
          val x54 = x53.!=(0)
          x54
        }
      }
      val x56 = x51.filter(x55)
      val x57 = new OptimalString(x56)
      val x58 = PARTRecord(x9, x25, x40, x41)
      val x59 = x4
      val x60 = x2.update(x59, x58)
      val x61 = x4
      val x62 = x61.+(1)
      val x63 = x4 = x62
      x63
    }
    val x65 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf1/partsupp.tbl")
    val x66 = new Array[PARTSUPPRecord](x65)
    val x67 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf1/partsupp.tbl")
    var x68: Int = 0
    val x91 = while({
      val x69 = x68
      val x70 = x69.<(x65)
      val x72 = x70.&&({
        val x71 = x67.hasNext()
        x71
      })
      x72
    })
    {
      val x73 = x67.next_int()
      val x74 = x67.next_int()
      val x75 = x67.next_int()
      val x76 = x67.next_double()
      val x78 = new Array[Byte](200)
      val x79 = x67.next(x78)
      val x82 = { x80: Byte => {
          val x81 = x80.!=(0)
          x81
        }
      }
      val x83 = x78.filter(x82)
      val x84 = new OptimalString(x83)
      val x85 = PARTSUPPRecord(x73, x74, x76)
      val x86 = x68
      val x87 = x66.update(x86, x85)
      val x88 = x68
      val x89 = x88.+(1)
      val x90 = x68 = x89
      x90
    }
    val x92 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf1/nation.tbl")
    val x93 = new Array[NATIONRecord](x92)
    val x94 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf1/nation.tbl")
    var x95: Int = 0
    val x124 = while({
      val x96 = x95
      val x97 = x96.<(x92)
      val x99 = x97.&&({
        val x98 = x94.hasNext()
        x98
      })
      x99
    })
    {
      val x100 = x94.next_int()
      val x102 = new Array[Byte](26)
      val x103 = x94.next(x102)
      val x106 = { x104: Byte => {
          val x105 = x104.!=(0)
          x105
        }
      }
      val x107 = x102.filter(x106)
      val x108 = new OptimalString(x107)
      val x109 = x94.next_int()
      val x111 = new Array[Byte](153)
      val x112 = x94.next(x111)
      val x115 = { x113: Byte => {
          val x114 = x113.!=(0)
          x114
        }
      }
      val x116 = x111.filter(x115)
      val x117 = new OptimalString(x116)
      val x118 = NATIONRecord(x100, x108, x109)
      val x119 = x95
      val x120 = x93.update(x119, x118)
      val x121 = x95
      val x122 = x121.+(1)
      val x123 = x95 = x122
      x123
    }
    val x125 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf1/region.tbl")
    val x126 = new Array[REGIONRecord](x125)
    val x127 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf1/region.tbl")
    var x128: Int = 0
    val x156 = while({
      val x129 = x128
      val x130 = x129.<(x125)
      val x132 = x130.&&({
        val x131 = x127.hasNext()
        x131
      })
      x132
    })
    {
      val x133 = x127.next_int()
      val x135 = new Array[Byte](26)
      val x136 = x127.next(x135)
      val x139 = { x137: Byte => {
          val x138 = x137.!=(0)
          x138
        }
      }
      val x140 = x135.filter(x139)
      val x141 = new OptimalString(x140)
      val x143 = new Array[Byte](153)
      val x144 = x127.next(x143)
      val x147 = { x145: Byte => {
          val x146 = x145.!=(0)
          x146
        }
      }
      val x148 = x143.filter(x147)
      val x149 = new OptimalString(x148)
      val x150 = REGIONRecord(x133, x141)
      val x151 = x128
      val x152 = x126.update(x151, x150)
      val x153 = x128
      val x154 = x153.+(1)
      val x155 = x128 = x154
      x155
    }
    val x157 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf1/supplier.tbl")
    val x158 = new Array[SUPPLIERRecord](x157)
    val x159 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf1/supplier.tbl")
    var x160: Int = 0
    val x206 = while({
      val x161 = x160
      val x162 = x161.<(x157)
      val x164 = x162.&&({
        val x163 = x159.hasNext()
        x163
      })
      x164
    })
    {
      val x165 = x159.next_int()
      val x167 = new Array[Byte](26)
      val x168 = x159.next(x167)
      val x171 = { x169: Byte => {
          val x170 = x169.!=(0)
          x170
        }
      }
      val x172 = x167.filter(x171)
      val x173 = new OptimalString(x172)
      val x175 = new Array[Byte](41)
      val x176 = x159.next(x175)
      val x179 = { x177: Byte => {
          val x178 = x177.!=(0)
          x178
        }
      }
      val x180 = x175.filter(x179)
      val x181 = new OptimalString(x180)
      val x182 = x159.next_int()
      val x184 = new Array[Byte](16)
      val x185 = x159.next(x184)
      val x188 = { x186: Byte => {
          val x187 = x186.!=(0)
          x187
        }
      }
      val x189 = x184.filter(x188)
      val x190 = new OptimalString(x189)
      val x191 = x159.next_double()
      val x193 = new Array[Byte](102)
      val x194 = x159.next(x193)
      val x197 = { x195: Byte => {
          val x196 = x195.!=(0)
          x196
        }
      }
      val x198 = x193.filter(x197)
      val x199 = new OptimalString(x198)
      val x200 = SUPPLIERRecord(x165, x173, x181, x182, x190, x191, x199)
      val x201 = x160
      val x202 = x158.update(x201, x200)
      val x203 = x160
      val x204 = x203.+(1)
      val x205 = x160 = x204
      x205
    }
    val x207 = new Range(0, 1, 1)
    val x863 = { x208: Int => {
        val x9470 = new Array[Set_REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord](1048576)
        val x862 = GenericEngine.runQuery({
          val x209 = GenericEngine.parseString("AFRICA")
          val x210 = GenericEngine.parseString("TIN")
          var x1078: Int = 0
          var x1083: Int = 0
          var x1106: Int = 0
          var x1129: Int = 0
          var x1163: Int = 0
          val x320 = { (x291: WindowRecord_Int_DynamicCompositeRecord_REGIONRecord_DynamicCompositeRecord_PARTRecord_DynamicCompositeRecord_NATIONRecord_DynamicCompositeRecord_SUPPLIERRecord_PARTSUPPRecord, x292: WindowRecord_Int_DynamicCompositeRecord_REGIONRecord_DynamicCompositeRecord_PARTRecord_DynamicCompositeRecord_NATIONRecord_DynamicCompositeRecord_SUPPLIERRecord_PARTSUPPRecord) => {
              val x293 = x291.wnd
              val x294 = x293.S_ACCTBAL
              val x295 = x292.wnd
              val x296 = x295.S_ACCTBAL
              val x297 = x294.<(x296)
              val x319 = if(x297) 
              {
                1
              }
              else
              {
                val x298 = x294.>(x296)
                val x318 = if(x298) 
                {
                  -1
                }
                else
                {
                  val x299 = x293.N_NAME
                  val x300 = x295.N_NAME
                  val x301 = x299.diff(x300)
                  var res302: Int = x301
                  val x303 = res302
                  val x304 = x303.==(0)
                  val x316 = if(x304) 
                  {
                    val x305 = x293.S_NAME
                    val x306 = x295.S_NAME
                    val x307 = x305.diff(x306)
                    val x308 = res302 = x307
                    val x309 = res302
                    val x310 = x309.==(0)
                    val x315 = if(x310) 
                    {
                      val x311 = x293.P_PARTKEY
                      val x312 = x295.P_PARTKEY
                      val x313 = x311.-(x312)
                      val x314 = res302 = x313
                      x314
                    }
                    else
                    {
                      ()
                    }
                    
                    x315
                  }
                  else
                  {
                    ()
                  }
                  
                  val x317 = res302
                  x317
                }
                
                x318
              }
              
              x319
            }
          }
          val x1239 = OrderingFactory(x320)
          val x1240 = new TreeSet()(x1239)
          var x1241: Boolean = false
          var j322: Int = 0
          var x1270: Int = 0
          val x806 = while({
            val x421 = true.&&({
              val x4087 = x1078
              val x420 = x4087.<(x65)
              x420
            })
            x421
          })
          {
            val x4089 = x1078
            val x423 = x66.apply(x4089)
            val x425 = x423.PS_SUPPKEY
            val x12826 = x425.-(1)
            val x12827 = x158.apply(x12826)
            val x12828 = x12827.S_SUPPKEY
            val x12829 = x12828.==(x425)
            val x12830 = if(x12829) 
            {
              val x6456 = x12827.S_SUPPKEY
              val x6457 = x6456.==(x425)
              val x6458 = if(x6457) 
              {
                val x1777 = x12827.S_NAME
                val x1778 = x12827.S_ADDRESS
                val x1779 = x12827.S_NATIONKEY
                val x1780 = x12827.S_PHONE
                val x1781 = x12827.S_ACCTBAL
                val x1782 = x12827.S_COMMENT
                val x1783 = x423.PS_PARTKEY
                val x1784 = x423.PS_SUPPLYCOST
                val x12842 = x93.apply(x1779)
                val x12843 = x12842.N_NATIONKEY
                val x12844 = x12843.==(x1779)
                val x12845 = if(x12844) 
                {
                  val x6472 = x12842.N_NATIONKEY
                  val x6473 = x6472.==(x1779)
                  val x6474 = if(x6473) 
                  {
                    val x1993 = x12842.N_NAME
                    val x1994 = x12842.N_REGIONKEY
                    val x12851 = x1783.-(1)
                    val x12852 = x2.apply(x12851)
                    val x12853 = x12852.P_SIZE
                    val x12854 = x12853.==(43)
                    val x12855 = x12854.&&({
                      val x376 = x12852.P_TYPE
                      val x377 = x376.endsWith(x210)
                      x377
                    })
                    val x12858 = if(x12855) 
                    {
                      val x379 = x12852.P_PARTKEY
                      val x6488 = x379.==(x1783)
                      val x6514 = if(x6488) 
                      {
                        val x6489 = x12852.P_PARTKEY
                        val x6490 = x6489.==(x1783)
                        val x6491 = if(x6490) 
                        {
                          val x2102 = x12852.P_MFGR
                          val x2103 = x12852.P_TYPE
                          val x2104 = x12852.P_SIZE
                          val x12868 = x126.apply(x1994)
                          val x12869 = x12868.R_NAME
                          val x12870 = x12869.===(x209)
                          val x12871 = if(x12870) 
                          {
                            val x358 = x12868.R_REGIONKEY
                            val x6502 = x358.==(x1994)
                            val x6510 = if(x6502) 
                            {
                              val x6503 = x12868.R_REGIONKEY
                              val x6504 = x6503.==(x1994)
                              val x6505 = if(x6504) 
                              {
                                val x2158 = x12868.R_NAME
                                val x783 = REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord(x6503, x2158, x6489, x2102, x2103, x2104, x6472, x1993, x1994, x6456, x1777, x1778, x1779, x1780, x1781, x1782, x1783, x425, x1784)
                                val x784 = x783.P_PARTKEY
                                val x8001 = x784.hashCode()
                                val x8002 = x8001.%(1048576)
                                val x9582 = x9470.apply(x8002)
                                var list9583: Set_REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord = x9582
                                val x9584 = list9583
                                val x8006 = x9584.==(null)
                                val x8011 = if(x8006) 
                                {
                                  val x9587 = new Array[REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord](256)
                                  val x9590 = Set_REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord(0, x9587)
                                  val x8008 = list9583 = x9590
                                  val x9592 = list9583
                                  val x8010 = x9470.update(x8002, x9592)
                                  x8010
                                }
                                else
                                {
                                  ()
                                }
                                
                                val x9594 = list9583
                                val x9595 = x9594.array
                                val x9596 = x9594.maxSize
                                val x9597 = x9595.update(x9596, x783)
                                val x9598 = x9594.maxSize
                                val x9599 = x9598.+(1)
                                val x9600 = x9594.maxSize_=(x9599)
                                ()
                              }
                              else
                              {
                                ()
                              }
                              
                              x6505
                            }
                            else
                            {
                              ()
                            }
                            
                            ()
                          }
                          else
                          {
                            ()
                          }
                          
                          val x12900 = x1163
                          val x12901 = x12900.+(1)
                          val x12902 = x1163 = x12901
                          ()
                        }
                        else
                        {
                          ()
                        }
                        
                        x6491
                      }
                      else
                      {
                        ()
                      }
                      
                      ()
                    }
                    else
                    {
                      ()
                    }
                    
                    val x12903 = x1129
                    val x12904 = x12903.+(1)
                    val x12905 = x1129 = x12904
                    ()
                  }
                  else
                  {
                    ()
                  }
                  
                  x6474
                }
                else
                {
                  ()
                }
                
                val x12906 = x1106
                val x12907 = x12906.+(1)
                val x12908 = x1106 = x12907
                ()
              }
              else
              {
                ()
              }
              
              x6458
            }
            else
            {
              ()
            }
            
            val x12909 = x1083
            val x12910 = x12909.+(1)
            val x12911 = x1083 = x12910
            val x4489 = x1078
            val x804 = x4489.+(1)
            val x4491 = x1078 = x804
            x4491
          }
          val x8038 = Range.apply(0, 1048576)
          val x8052 = { x8039: Int => {
              val x9619 = x9470.apply(x8039)
              val x8041 = x9619.!=(null)
              val x8051 = if(x8041) 
              {
                val x10038 = x9619.array
                val x10039 = x10038.apply(0)
                var x9629: REGIONRecord_PARTRecord_NATIONRecord_SUPPLIERRecord_PARTSUPPRecord = x10039
                val x9630 = x9629
                val x9631 = x9630.PS_SUPPLYCOST
                var x9632: Double = x9631
                val x10052 = x9619.maxSize
                val x10053 = Range.apply(0, x10052)
                val x10062 = { x10054: Int => {
                    val x10055 = x10038.apply(x10054)
                    val x10056 = x10055.PS_SUPPLYCOST
                    val x10057 = x9632
                    val x10058 = x10056.<(x10057)
                    val x10059 = if(x10058) 
                    {
                      val x9637 = x9629 = x10055
                      val x9638 = x9632 = x10056
                      x9638
                    }
                    else
                    {
                      ()
                    }
                    
                    x10059
                  }
                }
                val x10063 = x10053.foreach(x10062)
                val x9642 = x9629
                val x8049 = WindowRecord_Int_DynamicCompositeRecord_REGIONRecord_DynamicCompositeRecord_PARTRecord_DynamicCompositeRecord_NATIONRecord_DynamicCompositeRecord_SUPPLIERRecord_PARTSUPPRecord(x9642)
                val x8050 = x1240.+=(x8049)
                ()
              }
              else
              {
                ()
              }
              
              x8051
            }
          }
          val x8053 = x8038.foreach(x8052)
          val x859 = while({
            val x4506 = x1241
            val x823 = x4506.unary_!
            val x827 = x823.&&({
              val x825 = x1240.size
              val x826 = x825.!=(0)
              x826
            })
            x827
          })
          {
            val x829 = x1240.head
            val x830 = x1240.-=(x829)
            val x832 = j322
            val x833 = x832.<(100)
            val x834 = x833.==(false)
            val x858 = if(x834) 
            {
              val x4518 = x1241 = true
              x4518
            }
            else
            {
              val x836 = x829.wnd
              val x837 = x836.S_ACCTBAL
              val x838 = x836.S_NAME
              val x839 = x838.string
              val x840 = x836.N_NAME
              val x841 = x840.string
              val x842 = x836.P_PARTKEY
              val x843 = x836.P_MFGR
              val x844 = x843.string
              val x845 = x836.S_ADDRESS
              val x846 = x845.string
              val x847 = x836.S_PHONE
              val x848 = x847.string
              val x849 = x836.S_COMMENT
              val x850 = x849.string
              val x851 = printf("%.2f|%s|%s|%d|%s|%s|%s|%s\n", x837, x839, x841, x842, x844, x846, x848, x850)
              val x852 = j322
              val x853 = x852.+(1)
              val x854 = j322 = x853
              val x4538 = x1270
              val x856 = x4538.+(1)
              val x4540 = x1270 = x856
              x4540
            }
            
            x858
          }
          val x4541 = x1270
          val x861 = printf("(%d rows)\n", x4541)
          ()
        })
        x862
      }
    }
    val x864 = x207.foreach(x863)
    x864
  }
}