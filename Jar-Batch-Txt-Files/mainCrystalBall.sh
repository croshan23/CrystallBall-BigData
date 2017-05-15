# Script to Run CrystalBall Map Reduce program
echo ">>Initializing..."

hadoop fs -mkdir /user/cloudera/CrytalBallInput 

hadoop fs -put /home/cloudera/Desktop/amazonData.txt /user/cloudera/CrytalBallInput/

hadoop jar /home/cloudera/Desktop/CrystallBall.jar  CrystalBall /user/cloudera/CrytalBallInput