
class Main extends App {

    def main(args : String[]) : Unit = 

        System.out.println("Hello there") 

        val value1 = sum(1, 2) 
        val value2 = sumFunction(1)(2) 

        System.out.println(value1) 
        System.out.println(value2) 

    def sum(a : Int, b : Int) : Int = 

        a + b 
    
    def sumFunction(a : Int) : Int = 

        (b) => (a + b) 
}