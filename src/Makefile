
.PHONY: build run clean

build:
	@echo "Compiling Tema1..."
	javac -cp ".:lib/*" -d bin Tema1.java

run:
	java -cp ".:lib/*:bin" Tema1 $(ARGS)

clean:
	rm -rf bin/*
	rm -f *.txt