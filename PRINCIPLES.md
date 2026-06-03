## The Core Principles

While there are dozens of paradigms, almost all good software engineering boils down to a few core philosophies centered around maintainability, readability, and scalability.

### 1. SOLID Principles

This is the gold standard for object-oriented design. It prevents your codebase from becoming a tangled mess.

* **S - Single Responsibility Principle (SRP):** A class or function should have only one reason to change. If a function fetches data, parses it, and updates the UI, it's doing too much. Break it down.
* **O - Open/Closed Principle (OCP):** Code should be open for extension but closed for modification. You should be able to add new functionality (like a new payment gateway) by adding new code, not by altering existing, tested core logic.
* **L - Liskov Substitution Principle (LSP):** If you swap a base class with a subclass, the program shouldn't break. Subclasses must behave in expected ways without forcing the main code to check their specific type.
* **I - Interface Segregation Principle (ISP):** Don't force code to depend on methods it doesn't use. Create smaller, highly specific interfaces rather than one massive, generic one.
* **D - Dependency Inversion Principle (DIP):** High-level modules shouldn't depend on low-level modules; both should depend on abstractions (interfaces). This makes your code highly modular and easy to test.

### 2. The Simplicity Principles

* **KISS (Keep It Simple, Stupid):** Complexity is the enemy of maintainability. Always choose the simpler, more readable solution over the "clever" one. You write code once, but it will be read hundreds of times.
* **YAGNI (You Aren't Gonna Need It):** Do not build features, abstractions, or infrastructure for hypothetical future use cases. Solve today's problem today.

### 3. The Maintainability Principles

* **DRY (Don't Repeat Yourself):** Every piece of knowledge or logic must have a single, unambiguous representation in the system. If you find yourself copying and pasting code, abstract it into a reusable function or component.
* **Separation of Concerns (SoC):** Distinct features or logic (e.g., database access, business logic, user interface) should be isolated from one another.