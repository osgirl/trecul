/**
 * Copyright (c) 2012, Akamai Technologies
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *   Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * 
 *   Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 * 
 *   Neither the name of the Akamai Technologies nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <map>
#include <stdexcept>
#include <iostream>
#include "LLVMGen.h"
#include "CodeGenerationContext.hh"
#include "TypeCheckContext.hh"
#include "RecordType.hh"
#include "IQLGraphBuilder.hh"

const IQLToLLVMValue * unwrap(IQLToLLVMValueRef val) {
  return reinterpret_cast<const IQLToLLVMValue*>(val);
}

IQLToLLVMValueRef wrap(const IQLToLLVMValue * val) {
  return reinterpret_cast<IQLToLLVMValueRef>(val);
}

const IQLToLLVMLValue * unwrap(IQLToLLVMLValueRef val) {
  return reinterpret_cast<const IQLToLLVMLValue*>(val);
}

IQLToLLVMLValueRef wrap(const IQLToLLVMLValue * val) {
  return reinterpret_cast<IQLToLLVMLValueRef>(val);
}

IQLToLLVMRecordMapRef wrap(std::map<std::string, std::pair<std::string, const RecordType*> > * r)
{
  return reinterpret_cast<struct IQLToLLVMRecordMapStruct *>(r);
}

std::map<std::string, std::pair<std::string, const RecordType*> > * unwrap(IQLToLLVMRecordMapRef r)
{
  return reinterpret_cast<std::map<std::string, std::pair<std::string, const RecordType*> > *>(r);
}

std::vector<IQLToLLVMTypedValue> * unwrap(IQLToLLVMValueVectorRef v)
{
  return reinterpret_cast<std::vector<IQLToLLVMTypedValue> *>(v);
}

IQLToLLVMValueVectorRef wrap(std::vector<IQLToLLVMTypedValue> * ptr)
{
  return reinterpret_cast<IQLToLLVMValueVectorRef>(ptr);
}

class CodeGenerationContext * unwrap(IQLCodeGenerationContextRef ctxtRef)
{
  return reinterpret_cast<class CodeGenerationContext *> (ctxtRef);
}

IQLCodeGenerationContextRef wrap(class CodeGenerationContext * ctxt)
{
  return reinterpret_cast<IQLCodeGenerationContextRef> (ctxt);
}

IQLToLLVMValueVectorRef IQLToLLVMValueVectorCreate()
{
  return wrap(new std::vector<IQLToLLVMTypedValue>());
}

void IQLToLLVMValueVectorFree(IQLToLLVMValueVectorRef v)
{
  if (v == NULL) return;
  delete unwrap(v);
}

void IQLToLLVMValueVectorPushBack(IQLToLLVMValueVectorRef v, 
				  IQLToLLVMValueRef ft,
				  void * argAttrs)
{
  unwrap(v)->emplace_back(unwrap(ft), (const FieldType *) argAttrs);
}

////////////////////////////////////////////////////
// Here begins the actual external builder interface
////////////////////////////////////////////////////

void IQLToLLVMBuildDeclareLocal(IQLCodeGenerationContextRef ctxtRef, const char * nm, void * attrs) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildDeclareLocal(nm, (const FieldType * ) attrs);
}

void IQLToLLVMBuildLocalVariable(IQLCodeGenerationContextRef ctxtRef, const char * nm, IQLToLLVMValueRef init, void * attrs) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildLocalVariable(nm, unwrap(init), (const FieldType * ) attrs);
}

IQLToLLVMValueRef IQLToLLVMBuildHash(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueVectorRef lhs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildHash(*unwrap(lhs)));
}

IQLToLLVMValueRef IQLToLLVMBuildSortPrefix(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueVectorRef lhs, void * attr)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * retTy = (const FieldType *) attr;
  // Assume for the moment that one field generates
  // the entire prefix.
  std::vector<IQLToLLVMTypedValue> & args(*unwrap(lhs));
  return wrap(ctxt->buildSortPrefix(args, retTy));
}

IQLToLLVMValueRef IQLToLLVMBuildCall(IQLCodeGenerationContextRef ctxtRef, 
				     const char * f,
				     IQLToLLVMValueVectorRef lhs,
				     void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  // Call out to external function.  
  std::vector<IQLToLLVMTypedValue> & args(*unwrap(lhs));
  const FieldType * retType = (const FieldType *) retAttrs;
  return wrap(ctxt->buildCall(f, args, retType));
}

IQLToLLVMValueRef IQLToLLVMBuildCompare(IQLCodeGenerationContextRef ctxtRef, 
					IQLToLLVMValueRef lhs, 
					void * lhsAttributes, 
					IQLToLLVMValueRef rhs, 
					void * rhsAttributes,
					void * resultAttributes,
					IQLToLLVMPredicate op)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return wrap(ctxt->buildCompare(unwrap(lhs), lhsType, unwrap(rhs), rhsType, resultType, op));
}

IQLToLLVMValueRef IQLToLLVMBuildEquals(IQLCodeGenerationContextRef ctxtRef, 
				       IQLToLLVMValueRef lhs, 
				       void * lhsAttributes, 
				       IQLToLLVMValueRef rhs, 
				       void * rhsAttributes,
				       void * resultAttributes)
{
  return IQLToLLVMBuildCompare(ctxtRef, lhs, lhsAttributes, rhs, rhsAttributes,
			       resultAttributes, IQLToLLVMOpEQ);
}

IQLToLLVMValueRef IQLToLLVMBuildIsNull(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIsNull(unwrap(val)));
}

void IQLToLLVMBeginAnd(IQLCodeGenerationContextRef ctxtRef, void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildBeginAnd((const FieldType *) retAttrs);
}

void IQLToLLVMAddAnd(IQLCodeGenerationContextRef ctxtRef, 
		     IQLToLLVMValueRef lhs,
		     void * argAttrs,
		     void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildAddAnd(unwrap(lhs), (const FieldType *) argAttrs, (const FieldType *) retAttrs);
}

IQLToLLVMValueRef IQLToLLVMBuildAnd(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef rhs,
				    void * rhsAttrs,
				    void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildAnd(unwrap(rhs), (const FieldType *) rhsAttrs, (const FieldType *) retAttrs));
}

void IQLToLLVMBeginOr(IQLCodeGenerationContextRef ctxtRef, void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildBeginOr((const FieldType *) retAttrs);
}

void IQLToLLVMAddOr(IQLCodeGenerationContextRef ctxtRef, 
		     IQLToLLVMValueRef lhs,
		     void * argAttrs,
		     void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildAddOr(unwrap(lhs), (const FieldType *) argAttrs, (const FieldType *) retAttrs);
}

IQLToLLVMValueRef IQLToLLVMBuildOr(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef rhs,
				    void * rhsAttrs,
				    void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildOr(unwrap(rhs), (const FieldType *) rhsAttrs, (const FieldType *) retAttrs));
}

IQLToLLVMValueRef IQLToLLVMBuildNot(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef lhs,
				    void * argAttrs, void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return wrap(ctxt->buildNot(unwrap(lhs), lhsType, resultType));
}

IQLToLLVMValueRef IQLToLLVMBuildIsNull(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef lhs,
				       void * argAttrs, void * retAttrs, int isNotNull)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIsNull(unwrap(lhs), (const FieldType *) argAttrs, (const FieldType * ) retAttrs, isNotNull));
}

IQLToLLVMValueRef IQLToLLVMBuildCast(IQLCodeGenerationContextRef ctxtRef, 
				     IQLToLLVMValueRef e, 
				     void * argAttrs, void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildCast(unwrap(e), (const FieldType *) argAttrs, (const FieldType *) retAttrs));
}

IQLToLLVMValueRef IQLToLLVMBuildNegate(IQLCodeGenerationContextRef ctxtRef, 
				       IQLToLLVMValueRef e,
				       void * lhsAttributes,
				       void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return wrap(ctxt->buildNegate(unwrap(e), lhsType, resultType));
}

IQLToLLVMValueRef IQLToLLVMBuildAdd(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef lhs, 
				    void * lhsAttributes, 
				    IQLToLLVMValueRef rhs, void * rhsAttributes,
				    void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return wrap(ctxt->buildAdd(unwrap(lhs), lhsType, unwrap(rhs), rhsType, resultType));
}

IQLToLLVMValueRef IQLToLLVMBuildSub(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef lhs, 
				    void * lhsAttributes, 
				    IQLToLLVMValueRef rhs, 
				    void * rhsAttributes,
				    void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return wrap(ctxt->buildSub(unwrap(lhs), lhsType, unwrap(rhs), rhsType, resultType));
}

IQLToLLVMValueRef IQLToLLVMBuildMul(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef lhs, 
				    void * lhsAttributes, 
				    IQLToLLVMValueRef rhs, 
				    void * rhsAttributes,
				    void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return wrap(ctxt->buildMul(unwrap(lhs), lhsType, unwrap(rhs), rhsType, resultType));
}

IQLToLLVMValueRef IQLToLLVMBuildDiv(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef lhs, 
				    void * lhsAttributes, 
				    IQLToLLVMValueRef rhs, 
				    void * rhsAttributes,
				    void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return wrap(ctxt->buildDiv(unwrap(lhs), lhsType, unwrap(rhs), rhsType, resultType));
}

IQLToLLVMValueRef IQLToLLVMBuildMod(IQLCodeGenerationContextRef ctxtRef, 
				    IQLToLLVMValueRef lhs, 
				    void * lhsAttributes, 
				    IQLToLLVMValueRef rhs, 
				    void * rhsAttributes,
				    void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return wrap(ctxt->buildMod(unwrap(lhs), lhsType, unwrap(rhs), rhsType, resultType));
}

IQLToLLVMValueRef IQLToLLVMBuildBitwiseAnd(IQLCodeGenerationContextRef ctxtRef, 
					   IQLToLLVMValueRef lhs, 
					   void * lhsAttributes, 
					   IQLToLLVMValueRef rhs, 
					   void * rhsAttributes,
					   void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return wrap(ctxt->buildBitwiseAnd(unwrap(lhs), lhsType, unwrap(rhs), rhsType, resultType));
}

IQLToLLVMValueRef IQLToLLVMBuildBitwiseOr(IQLCodeGenerationContextRef ctxtRef, 
					  IQLToLLVMValueRef lhs, 
					  void * lhsAttributes, 
					  IQLToLLVMValueRef rhs, 
					  void * rhsAttributes,
					  void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return wrap(ctxt->buildBitwiseOr(unwrap(lhs), lhsType, unwrap(rhs), rhsType, resultType));
}

IQLToLLVMValueRef IQLToLLVMBuildBitwiseXor(IQLCodeGenerationContextRef ctxtRef, 
					   IQLToLLVMValueRef lhs, 
					   void * lhsAttributes, 
					   IQLToLLVMValueRef rhs, 
					   void * rhsAttributes,
					   void * resultAttributes)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) lhsAttributes;
  const FieldType * rhsType = (const FieldType *) rhsAttributes;
  const FieldType * resultType = (const FieldType *) resultAttributes;
  return wrap(ctxt->buildBitwiseXor(unwrap(lhs), lhsType, unwrap(rhs), rhsType, resultType));
}

IQLToLLVMValueRef IQLToLLVMBuildBitwiseNot(IQLCodeGenerationContextRef ctxtRef, 
					   IQLToLLVMValueRef lhs,
					   void * argAttrs, void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * lhsType = (const FieldType *) argAttrs;
  const FieldType * resultType = (const FieldType *) retAttrs;
  return wrap(ctxt->buildBitwiseNot(unwrap(lhs), lhsType, resultType));
}

void IQLToLLVMBuildReturnValue(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef iqlVal, void * retAttrs) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildReturnValue(unwrap(iqlVal), (const FieldType *) retAttrs);
}

IQLToLLVMValueRef IQLToLLVMBuildVariableRef(IQLCodeGenerationContextRef ctxtRef, 
					    const char * var,
					    const char * var2,
					    void * varAttrs) 
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildVariableRef(var, var2, (const FieldType *) varAttrs));
}

IQLToLLVMValueRef IQLToLLVMBuildArrayRef(IQLCodeGenerationContextRef ctxtRef, 
					 const char * var,
					 IQLToLLVMValueRef idx,
					 void * elementAttrs) 
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildArrayRef(var, unwrap(idx), (const FieldType *) elementAttrs));
}

IQLToLLVMValueRef IQLToLLVMBuildArray(IQLCodeGenerationContextRef ctxtRef, 
				      IQLToLLVMValueVectorRef lhs, void * arrayAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  std::vector<IQLToLLVMTypedValue> &vals(*unwrap(lhs));
  FieldType * ty = (FieldType *) arrayAttrs;
  return wrap(ctxt->buildArray(vals, ty));
}

IQLToLLVMLValueRef IQLToLLVMBuildLValue(IQLCodeGenerationContextRef ctxtRef,
					const char * var)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->lookup(var, NULL));
}

IQLToLLVMLValueRef IQLToLLVMBuildArrayLValue(IQLCodeGenerationContextRef ctxtRef, 
					     const char * var,
					     IQLToLLVMValueRef idx)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildArrayLValue(var, unwrap(idx)));
}

void IQLToLLVMCaseBlockBegin(IQLCodeGenerationContextRef ctxtRef, void * caseAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildCaseBlockBegin((const FieldType *) caseAttrs);
}

void IQLToLLVMCaseBlockIf(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef condVal)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildCaseBlockIf(unwrap(condVal));
}

void IQLToLLVMCaseBlockThen(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef value, void * valueAttrs, void * caseAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const FieldType * valueType = (const FieldType *) valueAttrs;
  const FieldType * caseType = (const FieldType *) caseAttrs;
  ctxt->buildCaseBlockThen(unwrap(value), valueType, caseType, false);
}

IQLToLLVMValueRef IQLToLLVMCaseBlockFinish(IQLCodeGenerationContextRef ctxtRef, void * caseAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildCaseBlockFinish((const FieldType *) caseAttrs));
}

void IQLToLLVMWhileBegin(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->whileBegin();
}

void IQLToLLVMWhileStatementBlock(IQLCodeGenerationContextRef ctxtRef, 
				  IQLToLLVMValueRef condVal, 
				  void * condAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->whileStatementBlock(unwrap(condVal), (const FieldType *) condAttrs);
}

void IQLToLLVMWhileFinish(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->whileFinish();
}

void IQLToLLVMBeginIfThenElse(IQLCodeGenerationContextRef ctxtRef, IQLToLLVMValueRef condVal)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildBeginIfThenElse(unwrap(condVal));
}

void IQLToLLVMElseIfThenElse(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildElseIfThenElse();
}

IQLToLLVMValueRef IQLToLLVMEndIfThenElse(IQLCodeGenerationContextRef ctxtRef, 
					 IQLToLLVMValueRef thenVal, void *thenAttrs,
					 IQLToLLVMValueRef elseVal, void *elseAttrs,
					 void * retAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildEndIfThenElse(unwrap(thenVal), (const FieldType *) thenAttrs, 
				       unwrap(elseVal), (const FieldType *) elseAttrs,
				       (const FieldType *) retAttrs));
}

void IQLToLLVMBeginSwitch(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildBeginSwitch();
}

void IQLToLLVMEndSwitch(IQLCodeGenerationContextRef ctxtRef, 
			IQLToLLVMValueRef switchExpr)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildEndSwitch(unwrap(switchExpr));
}

void IQLToLLVMBeginSwitchCase(IQLCodeGenerationContextRef ctxtRef, 
			      const char * caseVal)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildBeginSwitchCase(caseVal);
}

void IQLToLLVMEndSwitchCase(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildEndSwitchCase();
}

void IQLToLLVMBeginAggregateFunction(IQLCodeGenerationContextRef ctxtRef)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->restoreAggregateContext(&ctxt->Update);
}

IQLToLLVMValueRef IQLToLLVMBuildAggregateFunction(IQLCodeGenerationContextRef ctxtRef, 
						  const char * fn,
						  IQLToLLVMValueRef e,
						  void * attrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildAggregateFunction(fn, unwrap(e), (const FieldType *) attrs));
}

IQLToLLVMValueRef IQLToLLVMBuildInterval(IQLCodeGenerationContextRef ctxtRef,
					 const char * intervalType,
					 IQLToLLVMValueRef e)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildInterval(intervalType, unwrap(e)));
}

IQLToLLVMValueRef IQLToLLVMBuildLiteralCast(IQLCodeGenerationContextRef ctxtRef, 
					    const char * val,
					    const char * typeName)
{
  return wrap(unwrap(ctxtRef)->buildLiteralCast(val, typeName));
}

IQLToLLVMValueRef IQLToLLVMBuildDateLiteral(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDateLiteral(val));
}

IQLToLLVMValueRef IQLToLLVMBuildDatetimeLiteral(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDatetimeLiteral(val));
}

IQLToLLVMValueRef IQLToLLVMBuildDecimalInt32Literal(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDecimalInt32Literal(val));
}

IQLToLLVMValueRef IQLToLLVMBuildDecimalInt64Literal(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDecimalInt64Literal(val));
}

IQLToLLVMValueRef IQLToLLVMBuildFloatLiteral(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDoubleLiteral(val));
}

IQLToLLVMValueRef IQLToLLVMBuildVarcharLiteral(IQLCodeGenerationContextRef ctxtRef, const char * val) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildVarcharLiteral(val));
}

IQLToLLVMValueRef IQLToLLVMBuildDecimalLiteral(IQLCodeGenerationContextRef ctxtRef, const char * val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDecimalLiteral(val));
}

IQLToLLVMValueRef IQLToLLVMBuildTrue(IQLCodeGenerationContextRef ctxtRef) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildTrue());
}

IQLToLLVMValueRef IQLToLLVMBuildFalse(IQLCodeGenerationContextRef ctxtRef) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildFalse());
}

IQLToLLVMValueRef IQLToLLVMBuildNull(IQLCodeGenerationContextRef ctxtRef) {
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildNull());
}

void IQLToLLVMNotImplemented()
{
  throw std::runtime_error("IQLToLLVM: Not yet implemented");
}

void IQLToLLVMBuildSetNullableValue(IQLCodeGenerationContextRef ctxtRef,
				    IQLToLLVMLValueRef lvalRef,
				    IQLToLLVMValueRef val,
				    void * valAttrs,
				    void * lvalAttrs)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  const IQLToLLVMLValue * lval = unwrap(lvalRef);
  const FieldType * valTy = (const FieldType *) valAttrs;
  const FieldType * resultTy = (const FieldType *) lvalAttrs;
  ctxt->buildSetNullableValue(lval, unwrap(val), valTy, resultTy);
}

void LLVMSetField(IQLCodeGenerationContextRef ctxtRef, 
		  int * pos, IQLToLLVMValueRef val)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildSetField(pos, unwrap(val));
}

void LLVMSetFields(IQLCodeGenerationContextRef ctxtRef, 
		   const char * recordName, int * pos)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildSetFields(recordName, pos);
}

void LLVMBuildQuotedId(IQLCodeGenerationContextRef ctxtRef, 
		       const char * quotedId, const char * rename, int * pos)
{
  CodeGenerationContext * ctxt = unwrap(ctxtRef);
  ctxt->buildQuotedId(quotedId, rename, pos);
}

const FieldType * unwrap(IQLFieldTypeRef r)
{
  return reinterpret_cast<const FieldType *> (r);
}

IQLFieldTypeRef wrap(const FieldType * r)
{
  return reinterpret_cast<IQLFieldTypeRef> (r);
}

const RecordType * unwrap(IQLRecordTypeRef r)
{
  return reinterpret_cast<const RecordType *> (r);
}

IQLRecordTypeRef wrap(const RecordType * r)
{
  return reinterpret_cast<IQLRecordTypeRef> (r);
}

std::vector<const FieldType *> * unwrap(IQLFieldTypeVectorRef v)
{
  return reinterpret_cast<std::vector<const FieldType *> *>(v);
}

IQLFieldTypeVectorRef wrap(std::vector<const FieldType *> * ptr)
{
  return reinterpret_cast<IQLFieldTypeVectorRef>(ptr);
}

IQLFieldTypeVectorRef IQLFieldTypeVectorCreate()
{
  return wrap(new std::vector<const FieldType *>());
}

void IQLFieldTypeVectorFree(IQLFieldTypeVectorRef v)
{
  if (v == NULL) return;
  delete unwrap(v);
}

void IQLFieldTypeVectorPushBack(IQLFieldTypeVectorRef v, IQLFieldTypeRef ft)
{
  unwrap(v)->push_back(unwrap(ft));
}

void IQLTypeCheckLoadBuiltinFunctions(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->loadBuiltinFunctions();
}

void IQLTypeCheckBeginRecord(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->beginRecord();
}

void IQLTypeCheckAddFields(IQLTypeCheckContextRef ctxtRef, const char * recordName)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->addFields(recordName);
}

void IQLTypeCheckAddField(IQLTypeCheckContextRef ctxtRef, const char * name, IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->addField(name, unwrap(ty));
}

void IQLTypeCheckQuotedId(IQLTypeCheckContextRef ctxtRef, const char * id, const char * format)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->quotedId(id, format);
}

void IQLTypeCheckBuildRecord(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->buildRecord();
}

void IQLTypeCheckBuildLocal(IQLTypeCheckContextRef ctxtRef, const char * name, IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->buildLocal(name, unwrap(ty));
}

void IQLTypeCheckSetValue2(IQLTypeCheckContextRef ctxt, 
			   IQLFieldTypeRef lhs, 
			   IQLFieldTypeRef rhs)
{
  unwrap(ctxt)->buildSetValue(unwrap(lhs), unwrap(rhs));
}

IQLFieldTypeRef IQLTypeCheckArrayRef(IQLTypeCheckContextRef ctxt, 
				     const char * nm,
				     IQLFieldTypeRef idx)
{
  return wrap(unwrap(ctxt)->buildArrayRef(nm, unwrap(idx)));
}

IQLFieldTypeRef IQLTypeCheckBuildVariableRef(IQLTypeCheckContextRef ctxt, 
					     const char * nm,
					     const char * nm2)
{
  return wrap(unwrap(ctxt)->buildVariableRef(nm, nm2));
}

void IQLTypeCheckBeginSwitch(IQLTypeCheckContextRef ctxt, 
			     IQLFieldTypeRef e)
{
  unwrap(ctxt)->beginSwitch(unwrap(e));
}

IQLFieldTypeRef IQLTypeCheckModulus(IQLTypeCheckContextRef ctxt, 
				    IQLFieldTypeRef lhs, 
				    IQLFieldTypeRef rhs)
{
  return wrap(unwrap(ctxt)->buildModulus(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckSub(IQLTypeCheckContextRef ctxt, 
				    IQLFieldTypeRef lhs, 
				    IQLFieldTypeRef rhs)
{
  return wrap(unwrap(ctxt)->buildSub(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckAdditiveType(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildAdd(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckNegateType(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildNegate(unwrap(lhs)));
}

IQLFieldTypeRef IQLTypeCheckMultiplicativeType(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildMul(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckBitwiseType(IQLTypeCheckContextRef ctxtRef, 
					IQLFieldTypeRef lhs, 
					IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildBitwise(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckUnaryBitwiseType(IQLTypeCheckContextRef ctxtRef, 
					     IQLFieldTypeRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildBitwise(unwrap(lhs)));
}

IQLFieldTypeRef IQLTypeCheckHash(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeVectorRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildHash(*unwrap(lhs)));
}

IQLFieldTypeRef IQLTypeCheckSortPrefix(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeVectorRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildSortPrefix(*unwrap(lhs)));
}

IQLFieldTypeRef IQLTypeCheckCall(IQLTypeCheckContextRef ctxtRef, 
				 const char * f,
				 IQLFieldTypeVectorRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildCall(f, *unwrap(lhs)));
}

IQLFieldTypeRef IQLTypeCheckEquals(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildEquals(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckAnd(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildLogicalAnd(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckNot(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildLogicalNot(unwrap(lhs)));
}

IQLFieldTypeRef IQLTypeCheckRLike(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, IQLFieldTypeRef rhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildLike(unwrap(lhs), unwrap(rhs)));
}

IQLFieldTypeRef IQLTypeCheckIsNull(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef lhs, int isNotNull)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildBooleanType(false));
}

IQLFieldTypeRef IQLTypeCheckBuildInt32Type(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildInt32Type(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildInt64Type(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildInt64Type(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildDoubleType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDoubleType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildDecimalType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDecimalType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildDatetimeType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDatetimeType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildDateType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildDateType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildNVarcharType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  throw std::runtime_error("NVARCHAR type not yet supported in IQL");
}

IQLFieldTypeRef IQLTypeCheckBuildVarcharType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildVarcharType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildCharType(IQLTypeCheckContextRef ctxtRef,
					  const char * sz, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildCharType(sz, nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildBooleanType(IQLTypeCheckContextRef ctxtRef, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildBooleanType(nullable != 0));
}

IQLFieldTypeRef IQLTypeCheckBuildNilType(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildNilType());
}

IQLFieldTypeRef IQLTypeCheckBuildType(IQLTypeCheckContextRef ctxtRef,
				      const char * typeName, int nullable)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildType(typeName, nullable != 0));
}

void IQLTypeCheckBeginCase(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->beginCase();
}

void IQLTypeCheckAddCondition(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef cond)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->addCondition(unwrap(cond));
}

void IQLTypeCheckAddValue(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeRef val)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->addValue(unwrap(val));
}

IQLFieldTypeRef IQLTypeCheckBuildCase(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildCase());
}

IQLFieldTypeRef IQLTypeCheckIfThenElse(IQLTypeCheckContextRef ctxtRef, 
				       IQLFieldTypeRef condVal, 
				       IQLFieldTypeRef thenVal, 
				       IQLFieldTypeRef elseVal)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIfThenElse(unwrap(condVal),
				    unwrap(thenVal),
				    unwrap(elseVal)));
}

IQLFieldTypeRef IQLTypeCheckCast(IQLTypeCheckContextRef ctxtRef, 
				 IQLFieldTypeRef lhs, 
				 IQLFieldTypeRef target)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildCast(unwrap(lhs), unwrap(target)));
}

void IQLTypeCheckBeginAggregateFunction(IQLTypeCheckContextRef ctxtRef)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  ctxt->beginAggregateFunction();
}

IQLFieldTypeRef IQLTypeCheckBuildAggregateFunction(IQLTypeCheckContextRef ctxtRef, 
						   IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildAggregateFunction(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalDay(IQLTypeCheckContextRef ctxtRef, 
					     IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalDay(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalHour(IQLTypeCheckContextRef ctxtRef, 
					      IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalHour(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalMinute(IQLTypeCheckContextRef ctxtRef, 
						IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalMinute(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalMonth(IQLTypeCheckContextRef ctxtRef, 
					       IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalMonth(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalSecond(IQLTypeCheckContextRef ctxtRef, 
						IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalSecond(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildIntervalYear(IQLTypeCheckContextRef ctxtRef, 
					      IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildIntervalYear(unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckBuildInterval(IQLTypeCheckContextRef ctxtRef,
					 const char * intervalType,
					 IQLFieldTypeRef ty)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->buildInterval(intervalType, unwrap(ty)));
}

IQLFieldTypeRef IQLTypeCheckArray(IQLTypeCheckContextRef ctxtRef, IQLFieldTypeVectorRef lhs)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  const std::vector<const FieldType*> & ty(*unwrap(lhs));
  return wrap(ctxt->buildArray(ty));
}

IQLFieldTypeRef IQLTypeCheckSymbolTableGetType(IQLTypeCheckContextRef ctxtRef, const char * name)
{
  TypeCheckContext * ctxt = unwrap(ctxtRef);
  return wrap(ctxt->lookupType(name, NULL));
}

void IQLGraphNodeStart(IQLGraphContextRef ctxt, const char * type, const char * name)
{
  unwrap(ctxt)->nodeStart(type, name);
}

void IQLGraphNodeBuildIntegerParam(IQLGraphContextRef ctxt, const char * name, const char * val)
{
  unwrap(ctxt)->nodeAddIntegerParam(name, val);
}

void IQLGraphNodeBuildStringParam(IQLGraphContextRef ctxt, const char * name, const char * val)
{
  unwrap(ctxt)->nodeAddStringParam(name, val);
}

void IQLGraphNodeComplete(IQLGraphContextRef ctxt)
{
  unwrap(ctxt)->nodeComplete();
}

void IQLGraphNodeBuildEdge(IQLGraphContextRef ctxt, const char * from, const char * to)
{
  unwrap(ctxt)->edgeBuild(from, to);
}

class IQLGraphBuilder * unwrap(IQLGraphContextRef val)
{
  return reinterpret_cast<IQLGraphBuilder *>(val);
}

IQLGraphContextRef wrap(class IQLGraphBuilder * val)
{
  return reinterpret_cast<IQLGraphContextRef>(val);
}

void IQLRecordTypeBuildField(IQLRecordTypeContextRef ctxt, 
			     const char * name, 
			     IQLFieldTypeRef ty)
{
  if (NULL == ty) {
    throw std::runtime_error("Invalid primitive type");
  }
  const FieldType * ft = unwrap(ty);
  unwrap(ctxt)->buildField(name, ft);
}

class IQLRecordTypeBuilder * unwrap(IQLRecordTypeContextRef val)
{
  return reinterpret_cast<IQLRecordTypeBuilder *>(val);
}

IQLRecordTypeContextRef wrap(class IQLRecordTypeBuilder * val)
{
  return reinterpret_cast<IQLRecordTypeContextRef>(val);
}

class TypeCheckContext * unwrap(IQLTypeCheckContextRef ctxt)
{
  return reinterpret_cast<class TypeCheckContext *>(ctxt);
}

IQLTypeCheckContextRef wrap(class TypeCheckContext * val)
{
  return reinterpret_cast<IQLTypeCheckContextRef>(val);
}

