# class Expr:
#     def __init__(self, name):
#         self.name = name

#     def __str__(self):
#         return f"Expr<'{self.name}'>"
    
#     def __repr__(self):
#         return f"Expr<'{self.name}'>"
    
# class Literal(Expr):
#     def __init__(self, value):
#         self.value = value

#     def __str__(self):
#         return f"Literal<{self.value}>"
    
#     def __repr__(self):
#         return f"Literal<{self.value}>"

# class Column(Expr):
#     def __init__(self, name):
#         self.name = name

#     def __str__(self):
#         return f"Column<'{self.name}'>"
    
#     def __repr__(self):
#         return f"Column<'{self.name}'>"
    
#     def __greater__(self, other):
#         if not isinstance(other, Expr):
#             return GreaterThan(self, Literal(other))
#         return GreaterThan(self, other)
    
#     def __less__(self, other):
#         if not isinstance(other, Expr):
#             return GreaterThan(self, Literal(other))
#         return LessThan(self, other)
    
#     def __eq__(self, other):
#         if not isinstance(other, Expr):
#             return GreaterThan(self, Literal(other))
#         return Equal(self, other)

# class GreaterThan(Expr):
#     def __init__(self, left: Expr, right: Expr):
#         self.left = left
#         self.right = right

#     def __str__(self):
#         return f"GreaterThan<{self.left} {self.right}>"
    
#     def __repr__(self):
#         return f"GreaterThan<{self.left} {self.right}>"
    
# class LessThan(Expr):
#     def __init__(self, left: Expr, right: Expr):
#         self.left = left
#         self.right = right

#     def __str__(self):
#         return f"LessThan<{self.left} {self.right}>"
    
#     def __repr__(self):
#         return f"LessThan<{self.left} {self.right}>"
    
# class Equal(Expr):
#     def __init__(self, left: Expr, right: Expr):
#         self.left = left
#         self.right = right

#     def __str__(self):
#         return f"Equal<{self.left} {self.right}>"
    
#     def __repr__(self):
#         return f"Equal<{self.left} {self.right}>"
    
# class And(Expr):
#     def __init__(self, left: Expr, right: Expr):
#         self.left = left
#         self.right = right

#     def __str__(self):
#         return f"And<{self.left} {self.right}>"
    
#     def __repr__(self):
#         return f"And<{self.left} {self.right}>"
    
# class Or(Expr):
#     def __init__(self, left: Expr, right: Expr):
#         self.left = left
#         self.right = right

#     def __str__(self):
#         return f"Or<{self.left} {self.right}>"
    
#     def __repr__(self):
#         return f"Or<{self.left} {self.right}>"
    
# class Not(Expr):
#     def __init__(self, expr: Expr):
#         self.expr = expr

#     def __str__(self):
#         return f"Not<{self.expr}>"
    
#     def __repr__(self):
#         return f"Not<{self.expr}>"
    
# class Add(Expr):
#     def __init__(self, left: Expr, right: Expr):
#         self.left = left
#         self.right = right

#     def __str__(self):
#         return f"Add<{self.left} {self.right}>"
    
#     def __repr__(self):
#         return f"Add<{self.left} {self.right}>"
    
# class Subtract(Expr):
#     def __init__(self, left: Expr, right: Expr):
#         self.left = left
#         self.right = right

#     def __str__(self):
#         return f"Subtract<{self.left} {self.right}>"
    
#     def __repr__(self):
#         return f"Subtract<{self.left} {self.right}>"
    
# class Multiply(Expr):
#     def __init__(self, left: Expr, right: Expr):
#         self.left = left
#         self.right = right

#     def __str__(self):
#         return f"Multiply<{self.left} {self.right}>"
    
#     def __repr__(self):
#         return f"Multiply<{self.left} {self.right}>"
    
# class Divide(Expr):
#     def __init__(self, left: Expr, right: Expr):
#         self.left = left
#         self.right = right

#     def __str__(self):
#         return f"Divide<{self.left} {self.right}>"
    
#     def __repr__(self):
#         return f"Divide<{self.left} {self.right}>"
    
# class Modulo(Expr):
#     def __init__(self, left: Expr, right: Expr):
#         self.left = left
#         self.right = right

#     def __str__(self):
#         return f"Modulo<{self.left} {self.right}>"
    
#     def __repr__(self):
#         return f"Modulo<{self.left} {self.right}>"
    
# class Negate(Expr):
#     def __init__(self, expr: Expr):
#         self.expr = expr

#     def __str__(self):
#         return f"Negate<{self.expr}>"
    
#     def __repr__(self):
#         return f"Negate<{self.expr}>"