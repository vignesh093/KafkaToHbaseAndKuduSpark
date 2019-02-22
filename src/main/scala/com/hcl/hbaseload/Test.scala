package com.hcl.hbaseload
case class Person (firstName: String, lastName: String)
object Test {
  case class te(a:Double,b:Double)
  
   def main(args:Array[String]){
     
    val m = new ROSdata(1588858.0,1.47942544E9,1.06486082E8,-0.0179143730889,0.019995996167,-0.210257156298,0.0,0.0,0.0,1.42669999599,0.56981086731,12.0338525772)
    val RosDatastr = m.toString
     val rosdata = RosDatastr.substring(RosDatastr.indexOf("(")+1,RosDatastr.indexOf(")")).split(",")
          var count = 0
          println(rosdata)
          for(ind_data <- rosdata){
            if(count == 0)
              println("ind_data "+ind_data)
            else
              println("ind_data1 "+ind_data)
            count = count + 1
          }
   
     
   }
}