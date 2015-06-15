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


case class SortOp_Any(val sortedTree: TreeSet[AGGRecord_GroupByClass], val expectedSize: Int, var stop: Boolean)
case class LINEITEMRecord(val L_QUANTITY: Double, val L_EXTENDEDPRICE: Double, val L_DISCOUNT: Double, val L_TAX: Double, val L_RETURNFLAG: Char, val L_LINESTATUS: Char, val L_SHIPDATE: Int)
case class GroupByClass(val L_RETURNFLAG: Char, val L_LINESTATUS: Char)
case class Set_AGGRecord_GroupByClass(var maxSize: Int, val array: Array[AGGRecord_GroupByClass])
case class AGGRecord_GroupByClass(val key: GroupByClass, var aggs: Double, var aggs_1: Double, var aggs_2: Double, var aggs_3: Double, var aggs_4: Double, var aggs_5: Double, var aggs_6: Double, var aggs_7: Double, var aggs_8: Double)
object Q1 extends LegoRunner {
  def executeQuery(query: String, sf: Double, schema: ch.epfl.data.dblab.legobase.schema.Schema): Unit = main()
  def main(args: Array[String]) {
    run(args)
  }
  def main() = 
  {
    val x1 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf0.1/lineitem.tbl")
    val x2 = new Array[LINEITEMRecord](x1)
    val x3 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf0.1/lineitem.tbl")
    var x4: Int = 0
    val x52 = while({
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
      val x10 = x3.next_int()
      val x11 = x3.next_int()
      val x12 = x3.next_int()
      val x13 = x3.next_double()
      val x14 = x3.next_double()
      val x15 = x3.next_double()
      val x16 = x3.next_double()
      val x17 = x3.next_char()
      val x18 = x3.next_char()
      val x19 = x3.next_date
      val x20 = x3.next_date
      val x21 = x3.next_date
      val x23 = new Array[Byte](26)
      val x24 = x3.next(x23)
      val x27 = { x25: Byte => {
          val x26 = x25.!=(0)
          x26
        }
      }
      val x28 = x23.filter(x27)
      val x29 = new OptimalString(x28)
      val x31 = new Array[Byte](11)
      val x32 = x3.next(x31)
      val x35 = { x33: Byte => {
          val x34 = x33.!=(0)
          x34
        }
      }
      val x36 = x31.filter(x35)
      val x37 = new OptimalString(x36)
      val x39 = new Array[Byte](45)
      val x40 = x3.next(x39)
      val x43 = { x41: Byte => {
          val x42 = x41.!=(0)
          x42
        }
      }
      val x44 = x39.filter(x43)
      val x45 = new OptimalString(x44)
      val x46 = LINEITEMRecord(x13, x14, x15, x16, x17, x18, x19)
      val x47 = x4
      val x48 = x2.update(x47, x46)
      val x49 = x4
      val x50 = x49.+(1)
      val x51 = x4 = x50
      x51
    }
    val x53 = new Range(0, 1, 1)
    val x321 = { x54: Int => {
        var x2989: Int = 0
        val x3643 = new Array[Set_AGGRecord_GroupByClass](1048576)
        val x2991 = Range.apply(0, 1048576)
        val x2995 = { x2992: Int => {
            val x3647 = new Array[AGGRecord_GroupByClass](256)
            val x3650 = Set_AGGRecord_GroupByClass(0, x3647)
            val x2994 = x3643.update(x2992, x3650)
            x2994
          }
        }
        val x2996 = x2991.foreach(x2995)
        val x320 = GenericEngine.runQuery({
          val x55 = GenericEngine.parseDate("1998-09-02")
          var x381: Int = 0
          val x147 = { (x131: AGGRecord_GroupByClass, x132: AGGRecord_GroupByClass) => {
              val x133 = x131.key
              val x134 = x133.L_RETURNFLAG
              val x135 = x132.key
              val x136 = x135.L_RETURNFLAG
              val x137 = x134.-(x136)
              var res138: Int = x137
              val x139 = res138
              val x140 = x139.==(0)
              val x145 = if(x140) 
              {
                val x141 = x133.L_LINESTATUS
                val x142 = x135.L_LINESTATUS
                val x143 = x141.-(x142)
                val x144 = res138 = x143
                x144
              }
              else
              {
                ()
              }
              
              val x146 = res138
              x146
            }
          }
          val x486 = OrderingFactory(x147)
          val x487 = new TreeSet()(x486)
          val x148 = SortOp_Any(x487, 1024, false)
          var x508: Int = 0
          val x262 = while({
            val x170 = true.&&({
              val x1202 = x381
              val x169 = x1202.<(x1)
              x169
            })
            x170
          })
          {
            val x1204 = x381
            val x172 = x2.apply(x1204)
            val x174 = x172.L_SHIPDATE
            val x175 = x174.<=(x55)
            val x258 = if(x175) 
            {
              val x176 = x172.L_RETURNFLAG
              val x177 = x172.L_LINESTATUS
              val x178 = GroupByClass(x176, x177)
              val x3033 = x178.hashCode()
              val x3035 = x3033.&(1048575)
              val x3692 = x3643.apply(x3035)
              val x3037 = x2989
              val x3038 = x3035.>(x3037)
              val x3040 = if(x3038) 
              {
                val x3039 = x2989 = x3035
                x3039
              }
              else
              {
                ()
              }
              
              var x3701: Int = 0
              var x3702: Boolean = false
              val x3719 = while({
                val x3703 = x3701
                val x3704 = x3692.maxSize
                val x3705 = x3703.<(x3704)
                val x3708 = x3705.&&({
                  val x3706 = x3702
                  val x3707 = x3706.unary_!
                  x3707
                })
                x3708
              })
              {
                val x3709 = x3692.array
                val x3710 = x3701
                val x3711 = x3709.apply(x3710)
                val x3712 = x3711.key
                val x3713 = x3712.==(x178)
                val x3718 = if(x3713) 
                {
                  val x3714 = x3702 = true
                  x3714
                }
                else
                {
                  val x3715 = x3701
                  val x3716 = x3715.+(1)
                  val x3717 = x3701 = x3716
                  x3717
                }
                
                x3718
              }
              val x3720 = x3702
              val x3721 = x3720.unary_!
              val x3728 = if(x3721) 
              {
                null
              }
              else
              {
                val x3724 = x3692.array
                val x3725 = x3701
                val x3726 = x3724.apply(x3725)
                x3726
              }
              
              val x4992 = x3728.!=(null)
              val x3050 = if(x4992) 
              {
                x3728
              }
              else
              {
                var x6142: Double = 0
                var x6143: Double = 0
                var x6144: Double = 0
                var x6145: Double = 0
                var x6146: Double = 0
                var x6147: Double = 0
                var x6148: Double = 0
                var x6149: Double = 0
                var x6150: Double = 0
                var x6151: Double = 0
                val x6152 = x6151
                var x6153: Double = 0
                val x6154 = x6153
                var x6155: Double = 0
                val x6156 = x6155
                var x6157: Double = 0
                val x6158 = x6157
                var x6159: Double = 0
                val x6160 = x6159
                var x6161: Double = 0
                val x6162 = x6161
                var x6163: Double = 0
                val x6164 = x6163
                var x6165: Double = 0
                val x6166 = x6165
                var x6167: Double = 0
                val x6168 = x6167
                val x6169 = AGGRecord_GroupByClass(x178, x6152, x6154, x6156, x6158, x6160, x6162, x6164, x6166, x6168)
                val x3735 = x3692.array
                val x3736 = x3692.maxSize
                val x3737 = x3735.update(x3736, x6169)
                val x3738 = x3692.maxSize
                val x3739 = x3738.+(1)
                val x3740 = x3692.maxSize_=(x3739)
                x6169
              }
              
              val x6177 = x3050.aggs
              val x198 = x172.L_DISCOUNT
              val x199 = x198.+(x6177)
              val x6180 = x3050.aggs_=(x199)
              val x6181 = x3050.aggs_1
              val x208 = x172.L_QUANTITY
              val x209 = x208.+(x6181)
              val x6184 = x3050.aggs_1_=(x209)
              val x6185 = x3050.aggs_2
              val x218 = x172.L_EXTENDEDPRICE
              val x219 = x218.+(x6185)
              val x6188 = x3050.aggs_2_=(x219)
              val x6189 = x3050.aggs_3
              val x228 = 1.0.-(x198)
              val x229 = x218.*(x228)
              val x230 = x229.+(x6189)
              val x6193 = x3050.aggs_3_=(x230)
              val x6194 = x3050.aggs_4
              val x239 = x172.L_TAX
              val x240 = 1.0.+(x239)
              val x241 = x229.*(x240)
              val x242 = x241.+(x6194)
              val x6199 = x3050.aggs_4_=(x242)
              val x6200 = x3050.aggs_5
              val x252 = x6200.+(1.0)
              val x6202 = x3050.aggs_5_=(x252)
              ()
            }
            else
            {
              ()
            }
            
            val x1291 = x381
            val x260 = x1291.+(1)
            val x1293 = x381 = x260
            x1293
          }
          val x3101 = x2989
          val x3102 = x3101.+(1)
          val x3103 = Range.apply(0, x3102)
          val x3129 = { x3104: Int => {
              val x3776 = x3643.apply(x3104)
              val x3799 = x3776.maxSize
              val x3800 = Range.apply(0, x3799)
              val x3824 = { x3801: Int => {
                  val x3802 = x3776.array
                  val x3803 = x3802.apply(x3801)
                  val x6219 = x3803.aggs_1
                  val x6220 = x3803.aggs_5
                  val x3810 = x6219./(x6220)
                  val x6222 = x3803.aggs_6_=(x3810)
                  val x6223 = x3803.aggs_2
                  val x6224 = x3803.aggs_5
                  val x3814 = x6223./(x6224)
                  val x6226 = x3803.aggs_7_=(x3814)
                  val x6227 = x3803.aggs
                  val x6228 = x3803.aggs_5
                  val x3818 = x6227./(x6228)
                  val x6230 = x3803.aggs_8_=(x3818)
                  val x3820 = x148.!=(null)
                  val x3821 = if(x3820) 
                  {
                    val x286 = x148.sortedTree
                    val x287 = x286.+=(x3803)
                    ()
                  }
                  else
                  {
                    ()
                  }
                  
                  x3821
                }
              }
              val x3825 = x3800.foreach(x3824)
              x3825
            }
          }
          val x3130 = x3103.foreach(x3129)
          val x317 = while({
            val x291 = x148.stop
            val x292 = x291.unary_!
            val x296 = x292.&&({
              val x293 = x148.sortedTree
              val x294 = x293.size
              val x295 = x294.!=(0)
              x295
            })
            x296
          })
          {
            val x297 = x148.sortedTree
            val x298 = x297.head
            val x299 = x297.-=(x298)
            val x301 = x298.key
            val x302 = x301.L_RETURNFLAG
            val x303 = x301.L_LINESTATUS
            val x6251 = x298.aggs_1
            val x6252 = x298.aggs_2
            val x6253 = x298.aggs_3
            val x6254 = x298.aggs_4
            val x6255 = x298.aggs_6
            val x6256 = x298.aggs_7
            val x6257 = x298.aggs_8
            val x6258 = x298.aggs_5
            val x313 = printf("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.0f\n", x302, x303, x6251, x6252, x6253, x6254, x6255, x6256, x6257, x6258)
            val x1345 = x508
            val x315 = x1345.+(1)
            val x1347 = x508 = x315
            x1347
          }
          val x1348 = x508
          val x319 = printf("(%d rows)\n", x1348)
          ()
        })
        x320
      }
    }
    val x322 = x53.foreach(x321)
    x322
  }
}