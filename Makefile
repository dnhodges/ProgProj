all: 
	javac -d classes -classpath .:./javacc.jar:./je-3.3.82.jar *.java

run: 
	java -ea -classpath ./classes:./javacc.jar:./je-3.3.82.jar CommandLine Data

SQLParser.java: SQLParser.jj
	java -cp ./javacc.jar:./classes javacc SQLParser.jj

testparser:
	java -ea -classpath ./classes:./javacc.jar:./je-3.3.82.jar SQLParser

populate-states:
	cat populate-states.sql | java -ea -classpath ./classes:./javacc.jar:./je-3.3.82.jar CommandLine Data

query1:
	echo "SELECT * FROM states;" | java -ea -classpath ./classes:./javacc.jar:./je-3.3.82.jar CommandLine Data

query2:
	echo "SELECT * FROM states, senators where states.statecode = senators.statecode;" | java -ea -classpath ./classes:./javacc.jar:./je-3.3.82.jar CommandLine Data

query3:
	echo "SELECT senators.name, senators.born FROM states, senators where states.statecode = senators.statecode;" | java -ea -classpath ./classes:./javacc.jar:./je-3.3.82.jar CommandLine Data

query4:
	echo "SELECT counties.population FROM states, senators, counties where states.statecode = senators.statecode and counties.statecode = states.statecode and states.name = 'Maryland';" | java -ea -classpath ./classes:./javacc.jar:./je-3.3.82.jar CommandLine Data

query5:
	echo "SELECT senators.name, senators.born FROM states, senators where states.statecode = senators.statecode ORDER BY senators.born;" | java -ea -classpath ./classes:./javacc.jar:./je-3.3.82.jar CommandLine Data

query6:
	echo "SELECT distinct senators.born FROM states, senators where states.statecode = senators.statecode ORDER BY senators.born;" | java -ea -classpath ./classes:./javacc.jar:./je-3.3.82.jar CommandLine Data

query7:
	echo "SELECT states.name, senators.born, senators.name FROM states, senators where states.statecode = senators.statecode ORDER BY states.name, senators.born;" | java -ea -classpath ./classes:./javacc.jar:./je-3.3.82.jar CommandLine Data
