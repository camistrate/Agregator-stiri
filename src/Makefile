
.PHONY: build run clean

# Compilare cu Jackson pentru JSON parsing
build:
	@echo "Compiling Tema1..."
	javac -cp ".:lib/*" -d bin Tema1.java

# Rulare - primește ARGS ca parametri
run:
	java -cp ".:lib/*:bin" Tema1 $(ARGS)

# Curățare
clean:
	rm -rf bin/*