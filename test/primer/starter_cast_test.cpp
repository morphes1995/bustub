#include <bitset>
#include <exception>
#include <iostream>
#include <thread>  // NOLINT
#include "gtest/gtest.h"

using namespace std;
class Complex {
 public:
  Complex(double real = 0.0, double imag = 0.0) : m_real(real), m_imag(imag) {}

 public:
  operator double() const { return m_real; }  //类型转换函数
 private:
  double m_real;
  __attribute__((unused)) double m_imag;
};

TEST(StarterCastTest, StaticCastTest) {
  ////  //下面是正确的用法
  //    int m = 100;
  //    Complex c(12.5, 23.8);
  //    long n = static_cast<long>(m); //宽转换，没有信息丢失
  //  char ch = static_cast<char>(m); //窄转换，可能会丢失信息
  //  int *p1 = static_cast<int*>( malloc(10 * sizeof(int)) ); //将void指针转换为具体类型指针
  //  void *p2 = static_cast<void*>(p1); //将具体类型指针，转换为void指针
  //  double real= static_cast<double>(c); //调用类型转换函数
  ////  //下面的用法是错误的
  //  float *p3 = static_cast<float*>(p1); //不能在两个`具体`类型的 `指针`之间进行转换
  //  p3 = static_cast<float*>(0X2DF9); //不能将整数转换为指针类型
  //  int *i = nullptr;
  //  long *l = static_cast<long*>(i); // Static_cast from 'int *' to 'long *' is not allowed
}

TEST(StarterCastTest, ConstCastTest) {
  int a = 12;
  const int n = a;
  int *p = const_cast<int *>(&n);
  *p = 234;
  ASSERT_EQ(n, 234);
  ASSERT_EQ(*p, 234);

  const int n2 = 12;
  int *p2 = const_cast<int *>(&n);  // C++ 对常量的处理更像是编译时期的#define，是一个值替换的过程
  *p2 = 234;
  ASSERT_EQ(n2, 12);  // 代码中所有使用 n2 的地方在编译期间就被替换成了 100
  ASSERT_EQ(*p2, 234);
}

class MyBase {
 public:
  virtual void test() {}
};
class MyChild : public MyBase {};
TEST(StarterCastTest, ConstDynamicTest) {
  // dynamic cast
  MyBase *base = new MyBase();
  MyChild *child_p = dynamic_cast<MyChild *>(base);
  if (child_p == nullptr) std::cout << "Null pointer returned" << std::endl;
  try {
    __attribute__((unused)) MyChild &child = dynamic_cast<MyChild &>(*base);  // bad dynamic_cast
  } catch (std::bad_cast &e) {
    std::cout << e.what() << std::endl;  // bad dynamic_cast
  }

  MyChild a;
  MyBase &b = a;                                                    // Base ref to Child
  __attribute__((unused)) MyChild &c = dynamic_cast<MyChild &>(b);  // good ref cast

  MyBase a2;
  MyBase &b2 = a2;
  try {
    __attribute__((unused)) MyChild &c2 = dynamic_cast<MyChild &>(b2);  // bad ref dynamic_cast
  } catch (std::bad_cast &e) {
    std::cout << e.what() << std::endl;  // bad dynamic_cast
  }
}
class A {
 public:
  virtual void func() const { cout << "Class A" << endl; }

 private:
  __attribute__((unused)) int m_a;
};
class B : public A {
 public:
  virtual void func() const { cout << "Class B" << endl; }

 private:
  __attribute__((unused)) int m_b;
};
class C : public B {
 public:
  virtual void func() const { cout << "Class C" << endl; }

 private:
  __attribute__((unused)) int m_c;
};
class D : public C {
 public:
  virtual void func() const { cout << "Class D" << endl; }

 private:
  __attribute__((unused)) int m_d;
};

TEST(StarterCastTest, DynamicCastTest) {
  A *pa = new A();
  B *pb;
  C *pc;
  D *pd;
  //情况①
  pb = dynamic_cast<B *>(pa);  //向下转型失败
  if (pb == NULL) {
    cout << "Downcasting failed: A* to B*" << endl;
  } else {
    cout << "Downcasting successfully: A* to B*" << endl;
    pb->func();
  }
  pc = dynamic_cast<C *>(pa);  //向下转型失败
  if (pc == NULL) {
    cout << "Downcasting failed: A* to C*" << endl;
  } else {
    cout << "Downcasting successfully: A* to C*" << endl;
    pc->func();
  }
  cout << "-------------------------" << endl;
  //情况②
  pa = new D();                //向上转型都是允许的
  pb = dynamic_cast<B *>(pa);  //向下转型成功
  if (pb == NULL) {
    cout << "Downcasting failed: A* to B*" << endl;
  } else {
    cout << "Downcasting successfully: A* to B*" << endl;
    pb->func();
  }
  pc = dynamic_cast<C *>(pa);  //向下转型成功
  if (pc == NULL) {
    cout << "Downcasting failed: A* to C*" << endl;
  } else {
    cout << "Downcasting successfully: A* to C*" << endl;
    pc->func();
  }

  pd = dynamic_cast<D *>(pa);  //向下转型成功
  if (pd == NULL) {
    cout << "Downcasting failed: A* to D*" << endl;
  } else {
    cout << "Downcasting successfully: A* to D*" << endl;
    pd->func();
  }
}