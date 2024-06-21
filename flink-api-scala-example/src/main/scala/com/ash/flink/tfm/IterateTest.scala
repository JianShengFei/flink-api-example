package com.ash.flink.tfm

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object IterateTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDs: DataStream[String] = env.socketTextStream("localhost", 9999)

    import org.apache.flink.streaming.api.scala._
    val socketIntDs = socketDs.map(line => line.toInt)

    val result = socketIntDs.iterate(iteration => {
      //定义迭代体
      val minusOne: DataStream[Int] = iteration.map(v => {println("迭代体中value值为："+v);v - 1})
      //定义迭代条件，满足的继续迭代
      val stillGreaterThanZero: DataStream[Int] = minusOne.filter(_ > 0)
      //定义哪些数据最后进行输出
      val lessThanZero: DataStream[Int] = minusOne.filter(_ <= 0)
      //返回tuple，第一个参数是哪些数据流继续迭代，第二个参数是哪些数据流进行输出
      (stillGreaterThanZero, lessThanZero)
    })

    result.print()
    env.execute()


  }


}
