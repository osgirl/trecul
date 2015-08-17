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

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "CodeGenerationContext.hh"
#include "LLVMGen.h"
#include "RecordType.hh"
#include "TypeCheckContext.hh"

#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/GlobalVariable.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"

/**
 * Call a decimal binary operator.
 */

IQLToLLVMValue::IQLToLLVMValue (llvm::Value * val, 
				IQLToLLVMValue::ValueType globalOrLocal)
  :
  mValue(val),
  mIsNull(NULL),
  mValueType(globalOrLocal)
{
}
  
IQLToLLVMValue::IQLToLLVMValue (llvm::Value * val, llvm::Value * isNull, 
				IQLToLLVMValue::ValueType globalOrLocal)
  :
  mValue(val),
  mIsNull(isNull),
  mValueType(globalOrLocal)
{
}

llvm::Value * IQLToLLVMValue::getValue() const 
{ 
  return mValue; 
}

llvm::Value * IQLToLLVMValue::getNull() const 
{ 
  return mIsNull; 
}

void IQLToLLVMValue::setNull(llvm::Value * nv) 
{ 
  mIsNull = nv; 
}

bool IQLToLLVMValue::isLiteralNull() const 
{ 
  return mValue == NULL; 
}

IQLToLLVMValue::ValueType IQLToLLVMValue::getValueType() const 
{ 
  return mValueType; 
}

const IQLToLLVMValue * IQLToLLVMValue::get(CodeGenerationContext * ctxt, 
					   llvm::Value * val, 
					   IQLToLLVMValue::ValueType globalOrLocal)
{
  return get(ctxt, val, NULL, globalOrLocal);
}

const IQLToLLVMValue * IQLToLLVMValue::get(CodeGenerationContext * ctxt, 
					   llvm::Value * val,
					   llvm::Value * nv,
					   IQLToLLVMValue::ValueType globalOrLocal)
{
  ctxt->ValueFactory.push_back(new IQLToLLVMValue(val, nv, globalOrLocal));
  return ctxt->ValueFactory.back();
}

IQLToLLVMField::IQLToLLVMField(CodeGenerationContext * ctxt,
			       const RecordType * recordType,
			       const std::string& memberName,
			       const std::string& recordName)
  :
  mMemberName(memberName),
  mBasePointer(NULL),
  mRecordType(recordType)
{
  mBasePointer = ctxt->lookupValue(recordName.c_str(), NULL)->getValue();
}

IQLToLLVMField::IQLToLLVMField(const RecordType * recordType,
			       const std::string& memberName,
			       llvm::Value * basePointer)
  :
  mMemberName(memberName),
  mBasePointer(basePointer),
  mRecordType(recordType)
{
}

IQLToLLVMField::~IQLToLLVMField() 
{
}

void IQLToLLVMField::setNull(CodeGenerationContext *ctxt, bool isNull) const
{
  mRecordType->LLVMMemberSetNull(mMemberName, ctxt, mBasePointer, isNull);
}

const IQLToLLVMValue * IQLToLLVMField::getValuePointer(CodeGenerationContext * ctxt) const
{
  llvm::Value * outputVal = mRecordType->LLVMMemberGetPointer(mMemberName, 
							      ctxt, 
							      mBasePointer,
							      false);
  // Don't worry about Nullability, it is dealt separately  
  const IQLToLLVMValue * val = 
    IQLToLLVMValue::get(ctxt, 
			outputVal, 
			NULL, 
			IQLToLLVMValue::eGlobal);  
  return val;
}

const IQLToLLVMValue * 
IQLToLLVMField::getEntirePointer(CodeGenerationContext * ctxt) const
{
  llvm::Value * outputVal = mRecordType->LLVMMemberGetPointer(mMemberName, 
							      ctxt, 
							      mBasePointer,
							      false);
  llvm::Value * nullVal = NULL;
  if (isNullable()) {
    nullVal = mRecordType->LLVMMemberGetNull(mMemberName,
					     ctxt,
					     mBasePointer);
  }
  const IQLToLLVMValue * val = 
    IQLToLLVMValue::get(ctxt, 
			outputVal, 
			nullVal,
			IQLToLLVMValue::eGlobal);  
  return val;
}

bool IQLToLLVMField::isNullable() const
{
  const FieldType * outputTy = mRecordType->getMember(mMemberName).GetType();
  return outputTy->isNullable();
}

IQLToLLVMLocal::IQLToLLVMLocal(const IQLToLLVMValue * val,
			       llvm::Value * nullBit)
  :
  mValue(val),
  mNullBit(nullBit)
{
}

IQLToLLVMLocal::~IQLToLLVMLocal()
{
}

const IQLToLLVMValue * IQLToLLVMLocal::getValuePointer(CodeGenerationContext * ctxt) const
{
  return mValue;
}

const IQLToLLVMValue * 
IQLToLLVMLocal::getEntirePointer(CodeGenerationContext * ctxt) const
{
  if(NULL == mNullBit) {
    return mValue;
  } else {
    llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
    return IQLToLLVMValue::get(ctxt, mValue->getValue(),
			       b->CreateLoad(mNullBit),
			       mValue->getValueType());
  }
}

llvm::Value * IQLToLLVMLocal::getNullBitPointer() const
{
  return mNullBit;
}

void IQLToLLVMLocal::setNull(CodeGenerationContext * ctxt, bool isNull) const
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  b->CreateStore(isNull ? b->getTrue() : b->getFalse(), mNullBit);
}

bool IQLToLLVMLocal::isNullable() const
{
  return mNullBit != NULL;
}

SymbolTable::SymbolTable()
{
}

SymbolTable::~SymbolTable()
{
  for(table_type::iterator it = mSymbols.begin();
      it != mSymbols.end();
      ++it) {
    delete it->second;
  }
}

IQLToLLVMLValue * SymbolTable::lookup(const char * nm) const
{
  table_type::const_iterator it = mSymbols.find(nm);
  if (it == mSymbols.end() )
    return NULL;
  else
    return it->second;
}

void SymbolTable::add(const char * nm, IQLToLLVMLValue * value)
{
  // Don't bother worrying about overwriting a symbol table entry
  // this should be safe by virtue of type check. 
  // TODO: We shouldn't even be managing a symbol table during
  // code generation all names should be resolved during type
  // checking.
  // table_type::const_iterator it = mSymbols.find(nm);
  // if (it != mSymbols.end() )
  //   throw std::runtime_error((boost::format("Variable %1% already defined")
  // 			      % nm).str());
  mSymbols[nm] = value;
}

void SymbolTable::clear()
{
  mSymbols.clear();
}

void SymbolTable::dump() const
{
  // for(table_type::const_iterator it = tab.begin();
  //     it != tab.end();
  //     ++it) {
  //   std::cerr << it->first.c_str() << ":";
  //   llvm::unwrap(unwrap(it->second)->getValue())->dump();
  // }
}

CodeGenerationFunctionContext::CodeGenerationFunctionContext()
  :
  Builder(NULL),
  mSymbolTable(NULL),
  Function(NULL),
  RecordArguments(NULL),
  OutputRecord(NULL),
  AllocaCache(NULL)
{
}

CodeGenerationContext::CodeGenerationContext()
  :
  mOwnsModule(true),
  mSymbolTable(NULL),
  LLVMContext(NULL),
  LLVMModule(NULL),
  LLVMBuilder(NULL),
  LLVMDecContextPtrType(NULL),
  LLVMDecimal128Type(NULL),
  LLVMVarcharType(NULL),
  LLVMDatetimeType(NULL),
  LLVMFunction(NULL),
  IQLRecordArguments(NULL),
  IQLOutputRecord(NULL),
  LLVMMemcpyIntrinsic(NULL),
  LLVMMemsetIntrinsic(NULL),
  LLVMMemcmpIntrinsic(NULL),
  IQLMoveSemantics(0),
  IsIdentity(true),
  AggFn(0),
  AllocaCache(NULL)
{
  LLVMContext = ::LLVMContextCreate();
  LLVMModule = ::LLVMModuleCreateWithNameInContext("my cool JIT", 
						   LLVMContext);
}

CodeGenerationContext::~CodeGenerationContext()
{
  typedef std::vector<IQLToLLVMValue *> factory;
  for(factory::iterator it = ValueFactory.begin();
      it != ValueFactory.end();
      ++it) {
    delete *it;
  }

  if (LLVMBuilder) {
    LLVMDisposeBuilder(LLVMBuilder);
    LLVMBuilder = NULL;
  }
  if (mSymbolTable) {
    delete mSymbolTable;
    mSymbolTable = NULL;
  }  
  delete unwrap(IQLRecordArguments);
  if (mOwnsModule && LLVMModule) {
    LLVMDisposeModule(LLVMModule);
    LLVMModule = NULL;
  }
  if (LLVMContext) {
    LLVMContextDispose(LLVMContext);
    LLVMContext = NULL;
  }
  while(IQLCase.size()) {
    delete IQLCase.top();
    IQLCase.pop();
  }
}

void CodeGenerationContext::disownModule()
{
  mOwnsModule = false;
}

bool CodeGenerationContext::isValueType(const FieldType * ft)
{
  switch(ft->GetEnum()) {    
  case FieldType::VARCHAR:
  case FieldType::CHAR:
  case FieldType::FIXED_ARRAY:
  case FieldType::BIGDECIMAL:
  case FieldType::FUNCTION:
  case FieldType::NIL:
    return false;
  case FieldType::INT32:
  case FieldType::INT64:
  case FieldType::DOUBLE:
  case FieldType::DATETIME:
  case FieldType::DATE:
  case FieldType::INTERVAL:
    return true;
  default:
    throw std::runtime_error("INTERNAL ERROR: CodeGenerationContext::isValueType unexpected type");
  }
}

bool CodeGenerationContext::isPointerToValueType(llvm::Value * val, const FieldType * ft)
{
  return isValueType(ft) && llvm::Type::PointerTyID == val->getType()->getTypeID();
}

void CodeGenerationContext::defineVariable(const char * name,
					   llvm::Value * val,
					   llvm::Value * nullVal,
					   IQLToLLVMValue::ValueType globalOrLocal)
{
  const IQLToLLVMValue * tmp = IQLToLLVMValue::get(this, val, 
						   NULL, globalOrLocal);
  IQLToLLVMLocal * local = new IQLToLLVMLocal(tmp, nullVal);
  mSymbolTable->add(name, NULL, local);
}

void CodeGenerationContext::defineFieldVariable(llvm::Value * basePointer,
						const char * prefix,
						const char * memberName,
						const RecordType * recordType)
{
  IQLToLLVMField * field = new IQLToLLVMField(recordType,
					      memberName,
					      basePointer);
  mSymbolTable->add(prefix, memberName, field);
}

const IQLToLLVMLValue * 
CodeGenerationContext::lookup(const char * name, const char * name2)
{
  TreculSymbolTableEntry * lval = mSymbolTable->lookup(name, name2); 
  return lval->getValue();
}

const IQLToLLVMValue * 
CodeGenerationContext::lookupValue(const char * name, const char * name2)
{
  TreculSymbolTableEntry * lval = mSymbolTable->lookup(name, name2);
  return lval->getValue()->getEntirePointer(this);
}

const IQLToLLVMValue * 
CodeGenerationContext::lookupBasePointer(const char * name)
{
  TreculSymbolTableEntry * lval = mSymbolTable->lookup(name, NULL);
  return lval->getValue()->getEntirePointer(this);
}

llvm::Value * CodeGenerationContext::getContextArgumentRef()
{
  return lookupValue("__DecimalContext__", NULL)->getValue();
}

void CodeGenerationContext::reinitializeForTransfer()
{
  delete (local_cache *) AllocaCache;
  delete mSymbolTable;
  mSymbolTable = new TreculSymbolTable();
  AllocaCache = new local_cache();
}

void CodeGenerationContext::reinitialize()
{
  // Reinitialize and create transfer
  mSymbolTable->clear();
  LLVMFunction = NULL;
  unwrap(IQLRecordArguments)->clear();
}

void CodeGenerationContext::createFunctionContext()
{
  LLVMBuilder = LLVMCreateBuilderInContext(LLVMContext);
  mSymbolTable = new TreculSymbolTable();
  LLVMFunction = NULL;
  IQLRecordArguments = wrap(new std::map<std::string, std::pair<std::string, const RecordType*> >());
  IQLOutputRecord = NULL;
  AllocaCache = new local_cache();
}

void CodeGenerationContext::dumpSymbolTable()
{
  // mSymbolTable->dump();
}

void CodeGenerationContext::restoreAggregateContext(CodeGenerationFunctionContext * fCtxt)
{
  this->LLVMBuilder = fCtxt->Builder;
  this->mSymbolTable = fCtxt->mSymbolTable;
  this->LLVMFunction = fCtxt->Function;
  this->IQLRecordArguments = fCtxt->RecordArguments;
  this->IQLOutputRecord = fCtxt->OutputRecord;
  this->AllocaCache = (local_cache *) fCtxt->AllocaCache;
}

void CodeGenerationContext::saveAggregateContext(CodeGenerationFunctionContext * fCtxt)
{
  fCtxt->Builder = this->LLVMBuilder;
  fCtxt->mSymbolTable = this->mSymbolTable;
  fCtxt->Function = this->LLVMFunction;
  fCtxt->RecordArguments = this->IQLRecordArguments;
  fCtxt->OutputRecord = this->IQLOutputRecord;
  fCtxt->AllocaCache = this->AllocaCache;
}

void CodeGenerationContext::addInputRecordType(const char * name, 
					       const char * argumentName, 
					       const RecordType * rec)
{
  boost::dynamic_bitset<> mask;
  mask.resize(rec->size(), true);
  addInputRecordType(name, argumentName, rec, mask);
}

void CodeGenerationContext::addInputRecordType(const char * name, 
					       const char * argumentName, 
					       const RecordType * rec,
					       const boost::dynamic_bitset<>& mask)
{
  llvm::Value * basePointer = lookupValue(argumentName, NULL)->getValue();
  for(RecordType::const_member_iterator it = rec->begin_members();
      it != rec->end_members();
      ++it) {
    std::size_t idx = (std::size_t) std::distance(rec->begin_members(), it);
    if (!mask.test(idx)) continue;
    rec->LLVMMemberGetPointer(it->GetName(), 
			      this, 
			      basePointer,
			      true, // Put the member into the symbol table
			      name);
  }
  std::map<std::string, std::pair<std::string, const RecordType *> >& recordTypes(*unwrap(IQLRecordArguments));
  recordTypes[name] = std::make_pair(argumentName, rec);
}

llvm::Value * CodeGenerationContext::addExternalFunction(const char * treculName,
							 const char * implName,
							 llvm::Type * funTy)
{
  mTreculNameToSymbol[treculName] = implName;
  return llvm::Function::Create(llvm::dyn_cast<llvm::FunctionType>(funTy), 
				llvm::GlobalValue::ExternalLinkage,
				implName, llvm::unwrap(LLVMModule));
  
}

void CodeGenerationContext::buildDeclareLocal(const char * nm, const FieldType * ft)
{
  llvm::Type * ty = ft->LLVMGetType(this);
  // TODO: Check for duplicate declaration
  llvm::Value * allocAVal = buildEntryBlockAlloca(ty,nm);
  // NULL handling
  llvm::Value * nullVal = NULL;
  if (ft->isNullable()) {
    llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
    nullVal = buildEntryBlockAlloca(b->getInt1Ty(), 
					 (boost::format("%1%NullBit") %
					  nm).str().c_str());
    b->CreateStore(b->getFalse(), nullVal);
  } 

  defineVariable(nm, allocAVal, nullVal, IQLToLLVMValue::eLocal);
}

void CodeGenerationContext::buildLocalVariable(const char * nm, const IQLToLLVMValue * init, const FieldType * ft)
{
  // TODO: Temporary hack dealing with a special case in which the
  // initializing expression has already allocated a slot for the
  // lvalue.
  if (ft->GetEnum() == FieldType::FIXED_ARRAY && 
      !ft->isNullable() &&
      init->getValueType() == IQLToLLVMValue::eLocal) {
    defineVariable(nm, init->getValue(), NULL, IQLToLLVMValue::eLocal);    
  } else {
    // Allocate local
    buildDeclareLocal(nm, ft);
    // Create a temporary LValue object
    const IQLToLLVMLValue * localLVal = lookup(nm, NULL);
    // Set the value; no need to type promote since type of the
    // variable is inferred from the initializer
    buildSetNullableValue(localLVal, init, ft, false);
  }
}

void CodeGenerationContext::whileBegin()
{
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Create blocks for the condition, loop body and continue.
  std::stack<class IQLToLLVMStackRecord* > & stk(IQLStack);
  stk.push(new IQLToLLVMStackRecord());
  stk.top()->ThenBB = llvm::BasicBlock::Create(*c, "whileCond", TheFunction);
  stk.top()->ElseBB = llvm::BasicBlock::Create(*c, "whileBody");
  stk.top()->MergeBB = llvm::BasicBlock::Create(*c, "whileCont");

  // We do an unconditional branch to the condition block
  // so the loop has somewhere to branch to.
  b->CreateBr(stk.top()->ThenBB);
  b->SetInsertPoint(stk.top()->ThenBB);  
}

void CodeGenerationContext::whileStatementBlock(const IQLToLLVMValue * condVal,
						const FieldType * condTy)
{  
  // Test the condition and branch 
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Function * f = b->GetInsertBlock()->getParent();
  std::stack<class IQLToLLVMStackRecord* > & stk(IQLStack);
  f->getBasicBlockList().push_back(stk.top()->ElseBB);
  conditionalBranch(condVal, condTy, stk.top()->ElseBB, stk.top()->MergeBB);
  b->SetInsertPoint(stk.top()->ElseBB);
}

void CodeGenerationContext::whileFinish()
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Function * f = b->GetInsertBlock()->getParent();
  std::stack<class IQLToLLVMStackRecord* > & stk(IQLStack);

  // Branch to reevaluate loop predicate
  b->CreateBr(stk.top()->ThenBB);
  f->getBasicBlockList().push_back(stk.top()->MergeBB);
  b->SetInsertPoint(stk.top()->MergeBB);

  // Done with this entry
  delete stk.top();
  stk.pop();
}

void CodeGenerationContext::conditionalBranch(const IQLToLLVMValue * condVal,
					      const FieldType * condTy,
					      llvm::BasicBlock * trueBranch,
					      llvm::BasicBlock * falseBranch)
{
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Function * f = b->GetInsertBlock()->getParent();
  
  // Handle ternary logic here
  llvm::Value * nv = condVal->getNull();
  if (nv) {
    llvm::BasicBlock * notNullBB = llvm::BasicBlock::Create(*c, "notNull", f);
    b->CreateCondBr(b->CreateNot(nv), notNullBB, falseBranch);
    b->SetInsertPoint(notNullBB);
  }
  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(condVal->getValue(),
					  b->getInt32(0),
					  "boolCast");
  // Branch and set block
  b->CreateCondBr(boolVal, trueBranch, falseBranch);
}

const IQLToLLVMValue * 
CodeGenerationContext::buildArray(std::vector<IQLToLLVMTypedValue>& vals,
				  FieldType * arrayTy)
{
  // Detect if this is an array of constants
  // TODO: We need analysis or attributes that tell us whether the
  // array is const before we can make it static.  Right now we are just
  // making an generally invalid assumption that an array of numeric
  // constants is in fact const.
  bool isConstArray=true;
  for(std::vector<IQLToLLVMTypedValue>::iterator v = vals.begin(),
	e = vals.end(); v != e; ++v) {
    if (!llvm::isa<llvm::Constant>(v->getValue()->getValue())) {
      isConstArray = false;
      break;
    }
  }

  if (isConstArray) {
    return buildGlobalConstArray(vals, arrayTy);
  }

  // TODO: This is potentially inefficient.  Will LLVM remove the extra copy?
  // Even if it does, how much are we adding to the compilation time while
  // it cleans up our mess.
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Type * retTy = arrayTy->LLVMGetType(this);
  llvm::Value * result = buildEntryBlockAlloca(retTy, "nullableBinOp");    
  llvm::Type * ptrToElmntTy = llvm::cast<llvm::SequentialType>(retTy)->getElementType();
  ptrToElmntTy = llvm::PointerType::get(ptrToElmntTy, 0);
  llvm::Value * ptrToElmnt = b->CreateBitCast(result, ptrToElmntTy);
  // TODO: We are not allowing nullable element types for arrays at this point.
  int32_t sz = arrayTy->GetSize();
  for (int32_t i=0; i<sz; ++i) {
    // Make an LValue out of a slot in the array so we can
    // set the value into it.

    // GEP to get pointer to the correct offset.
    llvm::Value * gepIndexes[1] = { b->getInt64(i) };
    llvm::Value * lval = b->CreateInBoundsGEP(ptrToElmnt, 
					      llvm::ArrayRef<llvm::Value*>(&gepIndexes[0], 
									   &gepIndexes[1]));
    const IQLToLLVMValue * slot = IQLToLLVMValue::get(this, 
						      lval,
						      IQLToLLVMValue::eLocal);
    // TODO: type promotions???
    IQLToLLVMLocal localLVal(slot, NULL);
    buildSetNullableValue(&localLVal, vals[i].getValue(), 
			  dynamic_cast<const FixedArrayType*>(arrayTy)->getElementType(), false);
  }

  // return pointer to array
  return IQLToLLVMValue::get(this, result, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * 
CodeGenerationContext::buildGlobalConstArray(std::vector<IQLToLLVMTypedValue>& vals,
					     FieldType * arrayTy)
{
  llvm::Module * m = llvm::unwrap(LLVMModule);
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::ArrayType * arrayType = 
    llvm::dyn_cast<llvm::ArrayType>(arrayTy->LLVMGetType(this));
  BOOST_ASSERT(arrayType != NULL);
  llvm::GlobalVariable * globalArray = 
    new llvm::GlobalVariable(*m, arrayType, true, llvm::GlobalValue::InternalLinkage,
			  0, "constArray");
  globalArray->setAlignment(16);

  // Make initializer for the global.
  std::vector<llvm::Constant *> initializerArgs;
  for(std::vector<IQLToLLVMTypedValue>::const_iterator v = vals.begin(),
	e = vals.end(); v != e; ++v) {
    initializerArgs.push_back(llvm::cast<llvm::Constant>(v->getValue()->getValue()));
  }
  llvm::Constant * constArrayInitializer = 
    llvm::ConstantArray::get(arrayType, initializerArgs);
  globalArray->setInitializer(constArrayInitializer);

  
  return IQLToLLVMValue::get(this, globalArray, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildArrayRef(const char * var,
							    const IQLToLLVMValue * idx,
							    const FieldType * elementTy)
{
  const IQLToLLVMLValue * allocAVal=buildArrayLValue(var, idx);
  return buildRef(allocAVal->getEntirePointer(this), elementTy);
}

const IQLToLLVMLValue * CodeGenerationContext::buildArrayLValue(const char * var,
								const IQLToLLVMValue * idx)
{
  llvm::Module * m = llvm::unwrap(LLVMModule);
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Function *f = b->GetInsertBlock()->getParent();
  const IQLToLLVMValue * lvalue = lookupValue(var, NULL);
  llvm::Value * lval = lvalue->getValue();

  // Convert index to int64
  // TODO: Not handling NULL so type check should be enforcing this!
  llvm::Value * idxVal = idx->getValue();
  idxVal = b->CreateSExt(idxVal, b->getInt64Ty());

  // TODO: The "should" be a pointer to an array type and for us to GEP it
  // we need to bitcast to a pointer to the element type.
  // However...  There is a hack that we are being backward compatible
  // with for the moment that allows one to array reference a scalar
  // which is already a pointer to element type!
  const llvm::PointerType * ptrType = llvm::dyn_cast<llvm::PointerType>(lval->getType());
  BOOST_ASSERT(ptrType != NULL);
  const llvm::ArrayType * arrayType = llvm::dyn_cast<llvm::ArrayType>(ptrType->getElementType());
  if (arrayType) {    
    // Seeing about 50% performance overhead for the bounds checking.
    // Not implementing this until I have some optimization mechanism
    // for safely removing them in some cases (perhaps using LLVM).

    // llvm::Value * lowerBound = b->CreateICmpSLT(idxVal,
    // 						b->getInt64(0));
    // llvm::Value * upperBound = b->CreateICmpSGE(idxVal,
    // 						b->getInt64(arrayType->getNumElements()));
    // llvm::Value * cond = b->CreateOr(lowerBound, upperBound);
    // // TODO: Make a single module-wide basic block for the exceptional case?
    // llvm::BasicBlock * goodBlock = 
    //   llvm::BasicBlock::Create(*c, "arrayDereference", f);
    // llvm::BasicBlock * exceptionBlock = 
    //   llvm::BasicBlock::Create(*c, "arrayIndexException", f);
    // // Branch and set block
    // b->CreateCondBr(cond, exceptionBlock, goodBlock);
    // // Array out of bounds exception
    // b->SetInsertPoint(exceptionBlock); 
    // b->CreateCall(m->getFunction("InternalArrayException"));
    // // We should never make the branch since we actually
    // // throw in the called function.
    // b->CreateBr(goodBlock);

    // // Array check good: emit the value.
    // b->SetInsertPoint(goodBlock);  
    lval = b->CreateBitCast(lval, llvm::PointerType::get(arrayType->getElementType(),0));
  }
  
  // GEP to get pointer to the correct offset.
  llvm::Value * gepIndexes[1] = { idxVal };
  lval = b->CreateInBoundsGEP(lval, llvm::ArrayRef<llvm::Value*>(&gepIndexes[0], &gepIndexes[1]));
  return new IQLToLLVMLocal(IQLToLLVMValue::get(this, 
						lval,
						IQLToLLVMValue::eLocal),
			    NULL);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCall(const char * treculName,
				 const std::vector<IQLToLLVMTypedValue> & args,
				 llvm::Value * retTmp,
				 const FieldType * retType)
{
  // Get the implementation name of the function.
  std::map<std::string,std::string>::const_iterator it = mTreculNameToSymbol.find(treculName);
  if (mTreculNameToSymbol.end() == it) {
    throw std::runtime_error((boost::format("Unable to find implementation for "
					    "function %1%") % treculName).str());
  }
  llvm::LLVMContext & c(*llvm::unwrap(LLVMContext));
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);

  std::vector<LLVMValueRef> callArgs;
  LLVMValueRef fn = LLVMGetNamedFunction(LLVMModule, it->second.c_str());
  if (fn == NULL) {
    throw std::runtime_error((boost::format("Call to function %1% passed type checking "
					    "but implementation function %2% does not exist") %
			      treculName % it->second).str());
  }
  
  for(std::size_t i=0; i<args.size(); i++) {
    if (args[i].getType()->GetEnum() == FieldType::CHAR) {
      LLVMValueRef e = llvm::wrap(args[i].getValue()->getValue());
      // CHAR(N) arg must pass by reference
      // Pass as a pointer to int8.  pointer to char(N) is too specific
      // for a type signature.
      LLVMTypeRef int8Ptr = LLVMPointerType(LLVMInt8TypeInContext(LLVMContext), 0);
      LLVMValueRef ptr = LLVMBuildBitCast(LLVMBuilder, e, int8Ptr, "charcnvcasttmp1");
      callArgs.push_back(ptr);
    } else {
      callArgs.push_back(llvm::wrap(args[i].getValue()->getValue()));
    }
  }
  
  const llvm::Type * unwrapped = llvm::unwrap(LLVMTypeOf(fn));
  const llvm::PointerType * ptrTy = llvm::dyn_cast<llvm::PointerType>(unwrapped);
  const llvm::FunctionType * fnTy = llvm::dyn_cast<llvm::FunctionType>(ptrTy->getElementType());
  if (fnTy->getReturnType() == llvm::Type::getVoidTy(c)) {
    // Validate the calling convention.  If returning void must
    // also take RuntimeContext as last argument and take pointer
    // to return as next to last argument.
    if (callArgs.size() + 2 != fnTy->getNumParams() ||
	fnTy->getParamType(fnTy->getNumParams()-1) != 
	llvm::unwrap(LLVMDecContextPtrType) ||
	!fnTy->getParamType(fnTy->getNumParams()-2)->isPointerTy())
      throw std::runtime_error("Internal Error");

    const llvm::Type * retTy = retType->LLVMGetType(this);
    // The return type is determined by next to last argument.
    llvm::Type * retArgTy = llvm::cast<llvm::PointerType>(fnTy->getParamType(fnTy->getNumParams()-2))->getElementType();

    // Must alloca a value for the return value and pass as an arg.
    // No guarantee that the type of the formal of the function is exactly
    // the same as the LLVM ret type (in particular, CHAR(N) return
    // values will have a int8* formal) so we do a bitcast.
    LLVMValueRef retVal = llvm::wrap(retTmp);
    if (retTy != retArgTy) {
      const llvm::ArrayType * arrTy = llvm::dyn_cast<llvm::ArrayType>(retTy);
      if (retArgTy != b->getInt8Ty() ||
	  NULL == arrTy ||
	  arrTy->getElementType() != b->getInt8Ty()) {
	throw std::runtime_error("INTERNAL ERROR: mismatch between IQL function "
				 "return type and LLVM formal argument type.");
      }
      retVal = LLVMBuildBitCast(LLVMBuilder, 
				retVal,
				llvm::wrap(llvm::PointerType::get(retArgTy, 0)),
				"callReturnTempCast");
    }
    callArgs.push_back(retVal);					
    // Also must pass the context for allocating the string memory.
    callArgs.push_back(LLVMBuildLoad(LLVMBuilder, 
				     llvm::wrap(getContextArgumentRef()),
				     "ctxttmp"));    
    LLVMBuildCall(LLVMBuilder, 
		  fn, 
		  &callArgs[0], 
		  callArgs.size(), 
		  "");
    // Return was the second to last entry in the arg list.
    return IQLToLLVMValue::eLocal;
  } else {
    LLVMValueRef r = LLVMBuildCall(LLVMBuilder, 
				   fn, 
				   &callArgs[0], 
				   callArgs.size(), 
				   "call");
    b->CreateStore(llvm::unwrap(r), retTmp);
    return IQLToLLVMValue::eLocal;
  }
}

const IQLToLLVMValue *
CodeGenerationContext::buildCall(const char * f,
				 const std::vector<IQLToLLVMTypedValue> & args,
				 const FieldType * retType)
{
  if (boost::algorithm::iequals(f, "least")) {
    return buildLeastGreatest(args, retType, true);
  } else if (boost::algorithm::iequals(f, "greatest")) {
    return buildLeastGreatest(args, retType, false);
  } else if (boost::algorithm::iequals(f, "isnull") ||
	     boost::algorithm::iequals(f, "ifnull")) {
    return buildIsNullFunction(args, retType);
  } else {
    llvm::Type * retTy = retType->LLVMGetType(this);
    llvm::Value * retTmp = buildEntryBlockAlloca(retTy, "callReturnTemp");
    IQLToLLVMValue::ValueType vt = buildCall(f, args, retTmp, retType);
    // Get rid of unecessary temporary if applicable.
    retTmp = trimAlloca(retTmp, retType);
    if (isPointerToValueType(retTmp, retType)) {
      // Unlikely but possible to get here.  Pointers to 
      // value type are almost surely trimmed above.
      llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
      retTmp = b->CreateLoad(retTmp);
    }
    return IQLToLLVMValue::get(this, retTmp, vt);
  }
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastInt32(const IQLToLLVMValue * e, 
				      const FieldType * argType, 
				      llvm::Value * ret, 
				      const FieldType * retType)
{
  llvm::Value * e1 = e->getValue();
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT32:
    b->CreateStore(e1, ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::INT64:
    {
      llvm::Value * r = b->CreateTrunc(e1, 
				       b->getInt32Ty(),
				       "castInt64ToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::DOUBLE:
    {
      llvm::Value * r = b->CreateFPToSI(e1, 
					b->getInt32Ty(),
					"castDoubleToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::CHAR:
    {
      return buildCall("InternalInt32FromChar", args, ret, retType);
    }
  case FieldType::VARCHAR:
    {
      return buildCall("InternalInt32FromVarchar", args, ret, retType);
    }
  case FieldType::BIGDECIMAL:
    {
      return buildCall("InternalInt32FromDecimal", args, ret, retType);
    }
  case FieldType::DATE:
    {
      return buildCall("InternalInt32FromDate", args, ret, retType);
    }
  case FieldType::DATETIME:
    {
      return buildCall("InternalInt32FromDatetime", args, ret, retType);
    }
  default:
    // TODO: Cast INTEGER to DECIMAL
    throw std::runtime_error ((boost::format("Cast to INTEGER from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastInt32(const IQLToLLVMValue * e, 
							     const FieldType * argType, 
							     const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastInt32);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastInt64(const IQLToLLVMValue * e, 
				      const FieldType * argType, 
				      llvm::Value * ret, 
				      const FieldType * retType)
{
  llvm::Value * e1 = e->getValue();
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT32:
    {
      llvm::Value * r = b->CreateSExt(e1, 
				      b->getInt64Ty(),
				      "castInt64ToInt32");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::INT64:
    b->CreateStore(e1, ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::DOUBLE:
    {
      llvm::Value * r = b->CreateFPToSI(e1, 
					b->getInt64Ty(),
					"castDoubleToInt64");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::CHAR:
    {
      return buildCall("InternalInt64FromChar", args, ret, retType);
    }
  case FieldType::VARCHAR:
    {
      return buildCall("InternalInt64FromVarchar", args, ret, retType);
    }
  case FieldType::BIGDECIMAL:
    {
      return buildCall("InternalInt64FromDecimal", args, ret, retType);
    }
  case FieldType::DATE:
    {
      return buildCall("InternalInt64FromDate", args, ret, retType);
    }
  case FieldType::DATETIME:
    {
      return buildCall("InternalInt64FromDatetime", args, ret, retType);
    }
  default:
    // TODO: 
    throw std::runtime_error ((boost::format("Cast to BIGINT from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastInt64(const IQLToLLVMValue * e, 
							     const FieldType * argType, 
							     const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastInt64);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastDouble(const IQLToLLVMValue * e, 
				       const FieldType * argType, 
				       llvm::Value * ret, 
				       const FieldType * retType)
{
  llvm::Value * e1 = e->getValue();
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT32:
  case FieldType::INT64:
    {
      llvm::Value * r = b->CreateSIToFP(e1, 
					b->getDoubleTy(),
					"castIntToDouble");
      b->CreateStore(r, ret);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::DOUBLE:
    b->CreateStore(e1, ret);
    return IQLToLLVMValue::eLocal;
  case FieldType::CHAR:
    {
      return buildCall("InternalDoubleFromChar", args, ret, retType);
    }
  case FieldType::VARCHAR:
    {
      return buildCall("InternalDoubleFromVarchar", args, ret, retType);
    }
  case FieldType::BIGDECIMAL:
    {
      return buildCall("InternalDoubleFromDecimal", args, ret, retType);
    }
  case FieldType::DATE:
    {
      return buildCall("InternalDoubleFromDate", args, ret, retType);
    }
  case FieldType::DATETIME:
    {
      return buildCall("InternalDoubleFromDatetime", args, ret, retType);
    }
  default:
    throw std::runtime_error ((boost::format("Cast to DOUBLE PRECISION from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastDouble(const IQLToLLVMValue * e, 
							     const FieldType * argType, 
							     const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastDouble);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastDecimal(const IQLToLLVMValue * e, 
				       const FieldType * argType, 
				       llvm::Value * ret, 
				       const FieldType * retType)
{
  llvm::Value * e1 = e->getValue();
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT32:
    {
      return buildCall("InternalDecimalFromInt32", args, ret, retType);
    }
  case FieldType::INT64:
    {
      return buildCall("InternalDecimalFromInt64", args, ret, retType);
    }
  case FieldType::DOUBLE:
    {
      return buildCall("InternalDecimalFromDouble", args, ret, retType);
    }
  case FieldType::CHAR:
    {
      return buildCall("InternalDecimalFromChar", args, ret, retType);
    }
  case FieldType::VARCHAR:
    {
      return buildCall("InternalDecimalFromVarchar", args, ret, retType);
    }
  case FieldType::BIGDECIMAL:
    b->CreateStore(b->CreateLoad(e1), ret);
    return e->getValueType();
  case FieldType::DATE:
    {
      return buildCall("InternalDecimalFromDate", args, ret, retType);
    }
  case FieldType::DATETIME:
    {
      return buildCall("InternalDecimalFromDatetime", args, ret, retType);
    }
  default:
    throw std::runtime_error ((boost::format("Cast to DECIMAL from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastDecimal(const IQLToLLVMValue * e, 
							     const FieldType * argType, 
							     const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastDecimal);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastDate(const IQLToLLVMValue * e, 
				     const FieldType * argType, 
				     llvm::Value * ret, 
				     const FieldType * retType)
{
  llvm::Value * e1 = e->getValue();
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::VARCHAR:
    {
      return buildCall("InternalDateFromVarchar", args, ret, retType);
    }
  case FieldType::DATE:
    b->CreateStore(e1, ret);
    return e->getValueType();
  default:
    throw std::runtime_error ((boost::format("Cast to DATE from %1% not "
					     "implemented.") % 
			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastDate(const IQLToLLVMValue * e, 
							    const FieldType * argType, 
							    const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastDate);
}

// TODO: Support all directions of casting.
IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastDatetime(const IQLToLLVMValue * e, 
					 const FieldType * argType, 
					 llvm::Value * ret, 
					 const FieldType * retType)
{
  llvm::Value * e1 = e->getValue();
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::VARCHAR:
    {
      return buildCall("InternalDatetimeFromVarchar", args, ret, retType);
    }
  case FieldType::DATETIME:
    b->CreateStore(e1, ret);
    return e->getValueType();
  case FieldType::DATE:
    {
      return buildCall("InternalDatetimeFromDate", args, ret, retType);
    }
  default:
    throw std::runtime_error ((boost::format("Cast to DATE from %1% not "
  					     "implemented.") % 
  			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastDatetime(const IQLToLLVMValue * e, 
							     const FieldType * argType, 
							     const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastDatetime);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastChar(const IQLToLLVMValue * e, 
				     const FieldType * argType, 
				     llvm::Value * ret, 
				     const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * e1 = e->getValue();
  // Must bitcast to match calling convention.
  llvm::Type * int8Ptr = llvm::PointerType::get(b->getInt8Ty(), 0);
  llvm::Value * ptr = b->CreateBitCast(ret, int8Ptr);

  if (argType->GetEnum() == FieldType::CHAR) {
    int32_t toCopy=argType->GetSize() < retType->GetSize() ? argType->GetSize() : retType->GetSize();
    int32_t toSet=retType->GetSize() - toCopy;
    llvm::Value * args[5];
    if (toCopy > 0) {
      // memcpy arg1 at offset 0
      args[0] = ptr;
      args[1] = b->CreateBitCast(e1, int8Ptr);
      args[2] = b->getInt64(toCopy);
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(llvm::unwrap(LLVMMemcpyIntrinsic), llvm::makeArrayRef(&args[0], 5), "");
    }
    if (toSet > 0) {
      // memset spaces at offset toCopy
      args[0] = b->CreateGEP(ptr, b->getInt64(toCopy), "");;
      args[1] = b->getInt8(' ');
      args[2] = b->getInt64(toSet);
      args[3] = b->getInt32(1);
      args[4] = b->getInt1(0);
      b->CreateCall(llvm::unwrap(LLVMMemsetIntrinsic), llvm::makeArrayRef(&args[0], 5), "");
    }
    // Null terminate the string
    b->CreateStore(b->getInt8(0), b->CreateGEP(ptr, b->getInt64(retType->GetSize()), ""));
  } else if (argType->GetEnum() == FieldType::VARCHAR) {
    llvm::Value * callArgs[3];
    llvm::Value * fn = llvm::unwrap(LLVMModule)->getFunction("InternalCharFromVarchar");
    callArgs[0] = e1;
    callArgs[1] = ptr;
    callArgs[2] = b->getInt32(retType->GetSize());
    b->CreateCall(fn, llvm::makeArrayRef(&callArgs[0], 3), "");
  } else {
    throw std::runtime_error("Only supporting CAST from VARCHAR to CHAR(N)");
  }
  return IQLToLLVMValue::eLocal;
}

const IQLToLLVMValue * CodeGenerationContext::buildCastChar(const IQLToLLVMValue * e, 
							    const FieldType * argType, 
							    const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastChar);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCastVarchar(const IQLToLLVMValue * e, 
					const FieldType * argType, 
					llvm::Value * ret, 
					const FieldType * retType)
{
  llvm::Value * e1 = e->getValue();
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  std::vector<IQLToLLVMTypedValue> args;
  args.emplace_back(e, argType);

  switch(argType->GetEnum()) {
  case FieldType::INT32:
    return buildCall("InternalVarcharFromInt32", args, ret, retType);
  case FieldType::INT64:
    return buildCall("InternalVarcharFromInt64", args, ret, retType);
  case FieldType::DOUBLE:
    return buildCall("InternalVarcharFromDouble", args, ret, retType);
  case FieldType::CHAR:
    return buildCall("InternalVarcharFromChar", args, ret, retType);
  case FieldType::VARCHAR:
    {
      // Identity
      const IQLToLLVMValue * tgt = IQLToLLVMValue::get(this, ret, IQLToLLVMValue::eLocal);
      buildSetValue2(e, tgt, retType);
      return IQLToLLVMValue::eLocal;
    }
  case FieldType::BIGDECIMAL:
    return buildCall("InternalVarcharFromDecimal", args, ret, retType);
  case FieldType::DATE:
    return buildCall("InternalVarcharFromDate", args, ret, retType);
  case FieldType::DATETIME:
    return buildCall("InternalVarcharFromDatetime", args, ret, retType);
  default:
    throw std::runtime_error ((boost::format("Cast to VARCHAR from %1% not "
  					     "implemented.") % 
  			       retType->toString()).str());
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCastVarchar(const IQLToLLVMValue * e, 
							       const FieldType * argType, 
							       const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCastVarchar);
}

IQLToLLVMValue::ValueType CodeGenerationContext::buildCast(const IQLToLLVMValue * e, 
							   const FieldType * argType, 
							   llvm::Value * ret, 
							   const FieldType * retType)
{
  switch(retType->GetEnum()) {
  case FieldType::INT32:
    return buildCastInt32(e, argType, ret, retType);
  case FieldType::INT64:
    return buildCastInt64(e, argType, ret, retType);
  case FieldType::CHAR:
    return buildCastChar(e, argType, ret, retType);
  case FieldType::VARCHAR:
    return buildCastVarchar(e, argType, ret, retType);
  case FieldType::BIGDECIMAL:
    return buildCastDecimal(e, argType, ret, retType);
  case FieldType::DOUBLE:
    return buildCastDouble(e, argType, ret, retType);
  case FieldType::DATETIME:
    return buildCastDatetime(e, argType, ret, retType);
  case FieldType::DATE:
    return buildCastDate(e, argType, ret, retType);
  default:
    // Programming error; this should have been caught during type check.
    throw std::runtime_error("Invalid type cast");    
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCast(const IQLToLLVMValue * e, 
							const FieldType * argType, 
							const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCast);
}

const IQLToLLVMValue * CodeGenerationContext::buildCastNonNullable(const IQLToLLVMValue * e, 
								   const FieldType * argType, 
								   const FieldType * retType)
{
  return buildNonNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildCast);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildAdd(const IQLToLLVMValue * lhs, 
				const FieldType * lhsType, 
				const IQLToLLVMValue * rhs, 
				const FieldType * rhsType, 
				llvm::Value * ret, 
				const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  if ((lhsType != NULL && lhsType->GetEnum() == FieldType::INTERVAL) ||
      (rhsType != NULL && rhsType->GetEnum() == FieldType::INTERVAL)) {
    // Special case handling of datetime/interval addition.
    return buildDateAdd(lhs, lhsType, rhs, rhsType, ret, retType);
  }
  if ((lhsType != NULL && lhsType->GetEnum() == FieldType::CHAR)) {
    return buildCharAdd(lhs, lhsType, rhs, rhsType, ret, retType);
  }

  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue();
  llvm::Value * e2 = rhs->getValue();

  if (retType->isIntegral()) {
    llvm::Value * r = b->CreateAdd(e1, e2);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->isFloatingPoint()) {
    llvm::Value * r = b->CreateFAdd(e1, e2);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->GetEnum() == FieldType::BIGDECIMAL) {
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsType);
    args.emplace_back(rhs, rhsType);
    return buildCall("InternalDecimalAdd", args, ret, retType);
  } else if (retType->GetEnum() == FieldType::VARCHAR) {
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsType);
    args.emplace_back(rhs, rhsType);
    return buildCall("InternalVarcharAdd", args, ret, retType);
  } else {
    e1->getType()->dump();
    throw std::runtime_error("INTERNAL ERROR: Invalid Type");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildAdd(const IQLToLLVMValue * lhs, 
						       const FieldType * lhsType, 
						       const IQLToLLVMValue * rhs, 
						       const FieldType * rhsType, 
						       const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildAdd);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildSub(const IQLToLLVMValue * lhs, 
				const FieldType * lhsType, 
				const IQLToLLVMValue * rhs, 
				const FieldType * rhsType, 
				llvm::Value * ret, 
				const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  if (lhsType->GetEnum() == FieldType::DATE ||
      lhsType->GetEnum() == FieldType::DATETIME) {
    // Negate the interval value (which is integral)
    // then call add
    llvm::Value * neg = 
      b->CreateNeg(rhs->getValue());
    rhs = IQLToLLVMValue::get(this, neg, IQLToLLVMValue::eLocal);
    return buildDateAdd(lhs, lhsType, rhs, rhsType, ret, retType);
  } else {  
    lhs = buildCastNonNullable(lhs, lhsType, retType);
    rhs = buildCastNonNullable(rhs, rhsType, retType);
    llvm::Value * e1 = lhs->getValue();
    llvm::Value * e2 = rhs->getValue();
    if (retType->isIntegral()) {
      b->CreateStore(b->CreateSub(e1, e2), ret);
      return IQLToLLVMValue::eLocal;
    } else if (retType->isFloatingPoint()) {
      b->CreateStore(b->CreateFSub(e1, e2), ret);
      return IQLToLLVMValue::eLocal;
    } else if (retType->GetEnum() == FieldType::BIGDECIMAL) {
      std::vector<IQLToLLVMTypedValue> args;
      args.emplace_back(lhs, lhsType);
      args.emplace_back(rhs, rhsType);
      return buildCall("InternalDecimalSub", args, ret, retType);
    } else {
      throw std::runtime_error("INTERNAL ERROR: unexpected type in subtract");
    }
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildSub(const IQLToLLVMValue * lhs, 
						       const FieldType * lhsType, 
						       const IQLToLLVMValue * rhs, 
						       const FieldType * rhsType, 
						       const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildSub);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildMul(const IQLToLLVMValue * lhs, 
				const FieldType * lhsType, 
				const IQLToLLVMValue * rhs, 
				const FieldType * rhsType, 
				llvm::Value * ret, 
				const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue();
  llvm::Value * e2 = rhs->getValue();
  if (retType->isIntegral()) {
    b->CreateStore(b->CreateMul(e1, e2), ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->isFloatingPoint()) {
    b->CreateStore(b->CreateFMul(e1, e2), ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->GetEnum() == FieldType::BIGDECIMAL) {
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsType);
    args.emplace_back(rhs, rhsType);
    return buildCall("InternalDecimalMul", args, ret, retType);
  } else {
    throw std::runtime_error("INTERNAL ERROR: unexpected type in multiply");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildMul(const IQLToLLVMValue * lhs, 
						       const FieldType * lhsType, 
						       const IQLToLLVMValue * rhs, 
						       const FieldType * rhsType, 
						       const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildMul);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildDiv(const IQLToLLVMValue * lhs, 
				const FieldType * lhsType, 
				const IQLToLLVMValue * rhs, 
				const FieldType * rhsType, 
				llvm::Value * ret, 
				const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue();
  llvm::Value * e2 = rhs->getValue();
  if (retType->isIntegral()) {
    b->CreateStore(b->CreateSDiv(e1, e2), ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->isFloatingPoint()) {
    b->CreateStore(b->CreateFDiv(e1, e2), ret);
    return IQLToLLVMValue::eLocal;
  } else if (retType->GetEnum() == FieldType::BIGDECIMAL) {
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsType);
    args.emplace_back(rhs, rhsType);
    return buildCall("InternalDecimalDiv", args, ret, retType);
  } else {
    throw std::runtime_error("INTERNAL ERROR: unexpected type in multiply");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildDiv(const IQLToLLVMValue * lhs, 
						       const FieldType * lhsType, 
						       const IQLToLLVMValue * rhs, 
						       const FieldType * rhsType, 
						       const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildDiv);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildMod(const IQLToLLVMValue * lhs, 
				const FieldType * lhsType, 
				const IQLToLLVMValue * rhs, 
				const FieldType * rhsType, 
				llvm::Value * ret, 
				const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue();
  llvm::Value * e2 = rhs->getValue();
  if (retType->isIntegral()) {
    b->CreateStore(b->CreateSRem(e1, e2), ret);
    return IQLToLLVMValue::eLocal;
  } else {
    throw std::runtime_error("INTERNAL ERROR: unexpected type in modulus");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildMod(const IQLToLLVMValue * lhs, 
						       const FieldType * lhsType, 
						       const IQLToLLVMValue * rhs, 
						       const FieldType * rhsType, 
						       const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildMod);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildNegate(const IQLToLLVMValue * lhs,
				   const FieldType * lhsTy,
				   llvm::Value * ret,
				   const FieldType * retTy)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * e1 = lhs->getValue();
  if (retTy->isIntegral()) {
    llvm::Value * r = b->CreateNeg(e1);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else if (retTy->isFloatingPoint()) {
    llvm::Value * r = b->CreateFNeg(e1);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    std::vector<IQLToLLVMTypedValue> args;
    args.emplace_back(lhs, lhsTy);
    return buildCall("InternalDecimalNeg", args, ret, retTy);
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildNegate(const IQLToLLVMValue * e, 
							  const FieldType * argType, 
							  const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildNegate);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildDateAdd(const IQLToLLVMValue * lhs, 
				    const FieldType * lhsType, 
				    const IQLToLLVMValue * rhs, 
				    const FieldType * rhsType, 
				    llvm::Value * retVal, 
				    const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  if (lhsType != NULL && lhsType->GetEnum() == FieldType::INTERVAL) {
    std::swap(lhs, rhs);
    std::swap(lhsType, rhsType);
  }
  const IntervalType * intervalType = dynamic_cast<const IntervalType *>(rhsType);
  IntervalType::IntervalUnit unit = intervalType->getIntervalUnit();
  llvm::Value * callArgs[2];
  static const char * types [] = {"datetime", "date"};
  const char * ty = 
    lhsType->GetEnum() == FieldType::DATETIME ? types[0] : types[1];
  std::string 
    fnName((boost::format(
			  unit == IntervalType::DAY ? "%1%_add_day" : 
			  unit == IntervalType::HOUR ? "%1%_add_hour" :
			  unit == IntervalType::MINUTE ? "%1%_add_minute" :
			  unit == IntervalType::MONTH ? "%1%_add_month" :
			  unit == IntervalType::SECOND ? "%1%_add_second" :
			  "%1%_add_year") % ty).str());
  llvm::Value * fn = llvm::unwrap(LLVMModule)->getFunction(fnName.c_str());
  callArgs[0] = lhs->getValue();
  callArgs[1] = rhs->getValue();
  llvm::Value * ret = b->CreateCall(fn, llvm::makeArrayRef(&callArgs[0], 2), "");
  b->CreateStore(ret, retVal);
  return IQLToLLVMValue::eLocal;  
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCharAdd(const IQLToLLVMValue * lhs, 
				    const FieldType * lhsType, 
				    const IQLToLLVMValue * rhs, 
				    const FieldType * rhsType, 
				    llvm::Value * ret, 
				    const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * e1 = lhs->getValue();
  llvm::Value * e2 = rhs->getValue();

  llvm::Type * int8Ptr = b->getInt8PtrTy(0);
  unsigned lhsSz = getCharArrayLength(e1)-1;
  unsigned rhsSz = getCharArrayLength(e2)-1;
  // Allocate target storage and
  // memcpy the two char variables to the target at appropriate offsets.
  // Allocate storage for return value.  Bit cast for argument to memcpy
  llvm::Value * retPtrVal = b->CreateBitCast(ret, int8Ptr);
    
  // Get a pointer to int8_t for args
  llvm::Value * tmp1 = b->CreateBitCast(e1, int8Ptr);
  llvm::Value * tmp2 = b->CreateBitCast(e2, int8Ptr);

  llvm::Value * args[5];
  // memcpy arg1 at offset 0
  args[0] = retPtrVal;
  args[1] = tmp1;
  args[2] = b->getInt64(lhsSz);
  // TODO: Make use of alignment info to speed this up.  This assumption of 1 is pessimistics.
  args[3] = b->getInt32(1);
  args[4] = b->getInt1(0);
  b->CreateCall(llvm::unwrap(LLVMMemcpyIntrinsic), llvm::makeArrayRef(&args[0], 5), "");
  // memcpy arg2 at offset lhsSz (be sure to copy the trailing 0 to null terminate).
  args[0] = b->CreateGEP(retPtrVal, b->getInt64(lhsSz), "");
  args[1] = tmp2;
  args[2] = b->getInt64(rhsSz+1);
  // TODO: Make use of alignment info to speed this up.  This assumption of 1 is pessimistics.
  args[3] = b->getInt32(1);
  args[4] = b->getInt1(0);
  b->CreateCall(llvm::unwrap(LLVMMemcpyIntrinsic), llvm::makeArrayRef(&args[0], 5), "");
    
  return IQLToLLVMValue::eLocal;
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildBitwiseAnd(const IQLToLLVMValue * lhs, 
				       const FieldType * lhsType, 
				       const IQLToLLVMValue * rhs, 
				       const FieldType * rhsType, 
				       llvm::Value * ret, 
				       const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue();
  llvm::Value * e2 = rhs->getValue();
  if (retType->isIntegral()) {
    llvm::Value * r = b->CreateAnd(e1, e2);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    throw std::runtime_error("bitwise operations only supported for integer values");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildBitwiseAnd(const IQLToLLVMValue * lhs, 
							      const FieldType * lhsType, 
							      const IQLToLLVMValue * rhs, 
							      const FieldType * rhsType, 
							      const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildBitwiseAnd);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildBitwiseOr(const IQLToLLVMValue * lhs, 
				      const FieldType * lhsType, 
				      const IQLToLLVMValue * rhs, 
				      const FieldType * rhsType, 
				      llvm::Value * ret, 
				      const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue();
  llvm::Value * e2 = rhs->getValue();
  if (retType->isIntegral()) {
    llvm::Value * r = b->CreateOr(e1, e2);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    throw std::runtime_error("bitwise operations only supported for integer values");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildBitwiseOr(const IQLToLLVMValue * lhs, 
							     const FieldType * lhsType, 
							     const IQLToLLVMValue * rhs, 
							     const FieldType * rhsType, 
							     const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildBitwiseOr);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildBitwiseXor(const IQLToLLVMValue * lhs, 
				       const FieldType * lhsType, 
				       const IQLToLLVMValue * rhs, 
				       const FieldType * rhsType, 
				       llvm::Value * ret, 
				       const FieldType * retType)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  lhs = buildCastNonNullable(lhs, lhsType, retType);
  rhs = buildCastNonNullable(rhs, rhsType, retType);
  llvm::Value * e1 = lhs->getValue();
  llvm::Value * e2 = rhs->getValue();
  if (retType->isIntegral()) {
    llvm::Value * r = b->CreateXor(e1, e2);
    b->CreateStore(r, ret);
    return IQLToLLVMValue::eLocal;
  } else {
    throw std::runtime_error("bitwise operations only supported for integer values");
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildBitwiseXor(const IQLToLLVMValue * lhs, 
							      const FieldType * lhsType, 
							      const IQLToLLVMValue * rhs, 
							      const FieldType * rhsType, 
							      const FieldType * retType)
{
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, retType, &CodeGenerationContext::buildBitwiseXor);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildBitwiseNot(const IQLToLLVMValue * lhs,
				const FieldType * lhsTy,
				llvm::Value * ret,
				const FieldType * retTy)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * e1 = lhs->getValue();
  llvm::Value * r = b->CreateNot(e1);
  b->CreateStore(r, ret);
  return IQLToLLVMValue::eLocal;
}

const IQLToLLVMValue * CodeGenerationContext::buildBitwiseNot(const IQLToLLVMValue * e, 
						       const FieldType * argType, 
						       const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildBitwiseNot);
}

llvm::Value * 
CodeGenerationContext::buildVarcharIsSmall(llvm::Value * varcharPtr)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // Access first bit of the structure to see if large or small.
  llvm::Value * firstByte = 
    b->CreateLoad(b->CreateBitCast(varcharPtr, b->getInt8PtrTy()));
  return 
    b->CreateICmpEQ(b->CreateAnd(b->getInt8(1U),
				 firstByte),
		    b->getInt8(0U));
}

llvm::Value * 
CodeGenerationContext::buildVarcharGetSize(llvm::Value * varcharPtr)
{
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Function * f = b->GetInsertBlock()->getParent();

  llvm::Value * ret = b->CreateAlloca(b->getInt32Ty());
  
  llvm::BasicBlock * smallBB = llvm::BasicBlock::Create(*c, "small", f);
  llvm::BasicBlock * largeBB = llvm::BasicBlock::Create(*c, "large", f);
  llvm::BasicBlock * contBB = llvm::BasicBlock::Create(*c, "cont", f);
  
  b->CreateCondBr(buildVarcharIsSmall(varcharPtr), smallBB, largeBB);
  b->SetInsertPoint(smallBB);
  llvm::Value * firstByte = 
    b->CreateLoad(b->CreateBitCast(varcharPtr, b->getInt8PtrTy()));
  b->CreateStore(b->CreateSExt(b->CreateAShr(b->CreateAnd(b->getInt8(0xfe),
							  firstByte),  
					     b->getInt8(1U)),
			       b->getInt32Ty()),
		 ret);
  b->CreateBr(contBB);
  b->SetInsertPoint(largeBB);
  llvm::Value * firstDWord = b->CreateLoad(b->CreateStructGEP(varcharPtr, 0));
  b->CreateStore(b->CreateAShr(b->CreateAnd(b->getInt32(0xfffffffe),
					    firstDWord),  
			       b->getInt32(1U)),
		 ret);
  b->CreateBr(contBB);
  b->SetInsertPoint(contBB);
  return b->CreateLoad(ret);
}

llvm::Value * 
CodeGenerationContext::buildVarcharGetPtr(llvm::Value * varcharPtr)
{
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Function * f = b->GetInsertBlock()->getParent();

  llvm::Value * ret = b->CreateAlloca(b->getInt8PtrTy());
  
  llvm::BasicBlock * smallBB = llvm::BasicBlock::Create(*c, "small", f);
  llvm::BasicBlock * largeBB = llvm::BasicBlock::Create(*c, "large", f);
  llvm::BasicBlock * contBB = llvm::BasicBlock::Create(*c, "cont", f);
  
  b->CreateCondBr(buildVarcharIsSmall(varcharPtr), smallBB, largeBB);
  b->SetInsertPoint(smallBB);
  b->CreateStore(b->CreateConstGEP1_64(b->CreateBitCast(varcharPtr, 
							b->getInt8PtrTy()), 
				       1),
		 ret);
  b->CreateBr(contBB);
  b->SetInsertPoint(largeBB);
  b->CreateStore(b->CreateLoad(b->CreateStructGEP(varcharPtr, 2)), ret);
  b->CreateBr(contBB);
  b->SetInsertPoint(contBB);
  return b->CreateLoad(ret);
}

const IQLToLLVMValue * CodeGenerationContext::buildCompareResult(llvm::Value * boolVal)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);  
  llvm::Value * int32RetVal = b->CreateZExt(boolVal,
					    b->getInt32Ty(),
					    "cmpresultcast");
  return IQLToLLVMValue::get(this, int32RetVal, IQLToLLVMValue::eLocal);
}

IQLToLLVMValue::ValueType CodeGenerationContext::buildCompareResult(llvm::Value * boolVal,
								    llvm::Value * ret)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);  
  llvm::Value * int32RetVal = b->CreateZExt(boolVal,
					    b->getInt32Ty(),
					    "cmpresultcast");
  b->CreateStore(int32RetVal, ret);
  return IQLToLLVMValue::eLocal;
}

void CodeGenerationContext::buildMemcpy(llvm::Value * sourcePtr,
					const FieldAddress& sourceOffset,
					llvm::Value * targetPtr, 
					const FieldAddress& targetOffset,
					int64_t sz)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * args[5];
  args[0] = targetOffset.getPointer("memcpy_tgt", this, targetPtr);
  args[1] = sourceOffset.getPointer("memcpy_src", this, sourcePtr);
  args[2] = b->getInt64(sz);
  // TODO: Make use of alignment info to speed this up.  This assumption of 1 is pessimistics.
  args[3] = b->getInt32(1);
  args[4] = b->getInt1(1);
  b->CreateCall(llvm::unwrap(LLVMMemcpyIntrinsic), llvm::makeArrayRef(&args[0], 5), "");
}

void CodeGenerationContext::buildMemcpy(const std::string& sourceArg,
					const FieldAddress& sourceOffset,
					const std::string& targetArg, 
					const FieldAddress& targetOffset,
					int64_t sz)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * sourcePtr = b->CreateLoad(lookupBasePointer(sourceArg.c_str())->getValue(),
					  "srccpy");	
  llvm::Value * targetPtr = b->CreateLoad(lookupBasePointer(targetArg.c_str())->getValue(),
					  "tgtcpy");
  buildMemcpy(sourcePtr, sourceOffset, targetPtr, targetOffset, sz);
}

void CodeGenerationContext::buildMemset(llvm::Value * targetPtr,
					const FieldAddress& targetOffset,
					int8_t value,
					int64_t sz)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * args[5];
  // GEP to get pointers at offsets and then call the intrinsic.
  args[0] = targetOffset.getPointer("memset_tgt", this, targetPtr);
  args[1] = b->getInt8(value);
  args[2] = b->getInt64(sz);
  // TODO: Speed things up by making use of alignment info we have.
  args[3] = b->getInt32(1);
  args[4] = b->getInt1(0);
  b->CreateCall(llvm::unwrap(LLVMMemsetIntrinsic), llvm::makeArrayRef(&args[0], 5), "");
}

llvm::Value * CodeGenerationContext::buildMemcmp(llvm::Value * sourcePtr,
						 const FieldAddress& sourceOffset,
						 llvm::Value * targetPtr, 
						 const FieldAddress& targetOffset,
						 int64_t sz)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // Implement bit casting to the right argument types.
  if (llvm::Type::PointerTyID != sourcePtr->getType()->getTypeID())
    throw std::runtime_error("sourcePtr argument to memcmp not pointer type");
  if (llvm::Type::PointerTyID != targetPtr->getType()->getTypeID())
    throw std::runtime_error("targetPtr argument to memcmp not pointer type");
  // This is what we expect.
  llvm::Type * int8Ptr = llvm::PointerType::get(b->getInt8Ty(), 0);
  // If not already pointers to int8, bitcast to that
  if (sourcePtr->getType() != int8Ptr)
    sourcePtr = b->CreateBitCast(sourcePtr, int8Ptr, "memcmp_lhs_cvt");
  if (targetPtr->getType() != int8Ptr)
    targetPtr = b->CreateBitCast(targetPtr, int8Ptr, "memcmp_rhs_cvt");

  llvm::Value * args[3];
  // GEP to get pointers at offsets and then call the intrinsic.
  args[0] = targetOffset.getPointer("memcmp_lhs", this, targetPtr);
  args[1] = sourceOffset.getPointer("memcmp_rhs", this, sourcePtr);
  args[2] = b->getInt64(sz);
  
  return b->CreateCall(llvm::unwrap(LLVMMemcmpIntrinsic), llvm::makeArrayRef(&args[0], 3), "memcmp");
}

void CodeGenerationContext::buildBitcpy(const BitcpyOp& op,
					llvm::Value * sourcePtr,
					llvm::Value * targetPtr)
{
  // Implement shift as targetDword = targetDword&targetMask | ((sourceWord & sourceMask) >> N)
  // for an appropriate targetMask

  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  
  // Get int32 ptrs for source and target
  llvm::Type * int32PtrTy = llvm::PointerType::get(b->getInt32Ty(), 0);
  llvm::Value * source = op.mSourceOffset.getPointer("bitcpySource", this, 
						     sourcePtr);
  source = b->CreateBitCast(source, int32PtrTy);
  llvm::Value * target = op.mTargetOffset.getPointer("bitcpyTarget", this, 
						     targetPtr);
  target = b->CreateBitCast(target, int32PtrTy);

  if (op.mShift < 0) {
    // We are shift source to the right.  Get mask for target
    // bits that we should leave in place.  Be careful here to
    // get 1's in any shifted in positions of targetMask by shifting before
    // negating.
    int32_t sourceShift = -op.mShift;
    uint32_t targetMask = ~((op.mSourceBitmask) >> sourceShift);
    llvm::Value * maskedTgt = b->CreateAnd(b->CreateLoad(target),
					   b->getInt32(targetMask));
    llvm::Value * maskedSource = b->CreateAnd(b->CreateLoad(source),
					      b->getInt32(op.mSourceBitmask));
    maskedSource = b->CreateLShr(maskedSource, sourceShift);
    b->CreateStore(b->CreateOr(maskedTgt, maskedSource),
		   target);
  } else if (op.mShift > 0) {
    // We are shift source to the left
    int32_t sourceShift = op.mShift;
    uint32_t targetMask = ~((op.mSourceBitmask) << sourceShift);
    llvm::Value * maskedTgt = b->CreateAnd(b->CreateLoad(target),
					   b->getInt32(targetMask));
    llvm::Value * maskedSource = b->CreateAnd(b->CreateLoad(source),
					      b->getInt32(op.mSourceBitmask));
    maskedSource = b->CreateShl(maskedSource, sourceShift);
    b->CreateStore(b->CreateOr(maskedTgt, maskedSource),
		   target);
  } else {
    uint32_t targetMask = ~op.mSourceBitmask;
    llvm::Value * maskedTgt = b->CreateAnd(b->CreateLoad(target),
					   b->getInt32(targetMask));
    llvm::Value * maskedSource = b->CreateAnd(b->CreateLoad(source),
					      b->getInt32(op.mSourceBitmask));
    b->CreateStore(b->CreateOr(maskedTgt, maskedSource),
		   target);
  }
}

void CodeGenerationContext::buildBitset(const BitsetOp& op,
					llvm::Value * targetPtr)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  
  // Get int32 ptr for target
  llvm::Type * int32PtrTy = llvm::PointerType::get(b->getInt32Ty(), 0);
  llvm::Value * target = op.mTargetOffset.getPointer("bitsetTarget", this, 
						     targetPtr);
  target = b->CreateBitCast(target, int32PtrTy);

  // Set bits in bitmask preserving any bits outside mask that
  // are already set.
  llvm::Value * tgt = b->CreateLoad(target);
  llvm::Value * newBits = b->getInt32(op.mTargetBitmask);
  b->CreateStore(b->CreateOr(tgt, newBits),
		 target);
}

void CodeGenerationContext::buildSetFieldsRegex(const std::string& sourceName,
						const RecordType * sourceType,
						const std::string& expr,
						const std::string& rename,
						const std::string& recordName,
						int * pos)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);

  llvm::Value * sourcePtr = b->CreateLoad(lookupBasePointer(sourceName.c_str())->getValue());
  llvm::Value * targetPtr = b->CreateLoad(lookupBasePointer("__OutputPointer__")->getValue());
			       
  RecordTypeCopy c(sourceType,
		   unwrap(IQLOutputRecord),
		   expr,
		   rename, 
		   pos);

  // Perform bitset as needed
  for(std::vector<BitsetOp>::const_iterator opit = c.getBitset().begin();
      opit != c.getBitset().end();
      ++opit) {
    buildBitset(*opit, targetPtr);			  
  }
  // Perform bitcpy as needed
  for(std::vector<BitcpyOp>::const_iterator opit = c.getBitcpy().begin();
      opit != c.getBitcpy().end();
      ++opit) {
    buildBitcpy(*opit, sourcePtr, targetPtr);
  }
  // Perform memcpy's as needed.
  // Explicitly copy fields as needed.
  for(std::vector<MemcpyOp>::const_iterator opit = c.getMemcpy().begin();
      opit != c.getMemcpy().end();
      ++opit) {
    buildMemcpy(sourcePtr,
		opit->mSourceOffset,
		targetPtr,
		opit->mTargetOffset,
		opit->mSize);
			  
  }
  for(RecordTypeCopy::set_type::const_iterator fit = c.getSet().begin();
      fit != c.getSet().end();
      ++fit) {
    int tmp=fit->second;
    buildSetField(&tmp, 
		  buildVariableRef(recordName.size() ? recordName.c_str() : fit->first.GetName().c_str(), 
				   recordName.size() ? fit->first.GetName().c_str() : NULL,
				   fit->first.GetType()));
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildRef(const IQLToLLVMValue * allocAVal,
						       const FieldType * resultTy)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  /* Two cases here.  Value types (8 bytes and less) are loaded whereas larger types 
     such as varchar and decimal are passed by reference.  Values that are value are always local */
  if (isPointerToValueType(allocAVal->getValue(), resultTy)) {
    //std::cout << "Loading variable " << var << "\n";
    return IQLToLLVMValue::get(this, 
			       b->CreateLoad(allocAVal->getValue()),
			       allocAVal->getNull(),
			       IQLToLLVMValue::eLocal);
  } else {
    //std::cout << "Variable reference " << var << "\n";
    return allocAVal;
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildVariableRef(const char * var,
							       const char * var2,
							       const FieldType * varTy)
{
  /* Lookup value in symbol table */
  /* TODO: Handle undefined reference */
  const IQLToLLVMValue * allocAVal = lookupValue(var, var2);
  return buildRef (allocAVal, varTy);
}

const IQLToLLVMValue * CodeGenerationContext::buildNullableUnaryOp(const IQLToLLVMValue * lhs, 
								   const FieldType * lhsType, 
								   const FieldType * resultType,
								   UnaryOperatorMemFn unOp)
{
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * n1 = lhs->getNull();
  BOOST_ASSERT((lhsType->isNullable() && n1 != NULL) ||
	       (!lhsType->isNullable() && n1 == NULL));
  if (lhs->isLiteralNull()) {
    return buildNull();
  }
  llvm::Value * nv = NULL;
  // Allocate space for a result
  llvm::Type * retTy = resultType->LLVMGetType(this);
  llvm::Value * result = buildEntryBlockAlloca(retTy, "nullableUnOp");    
  // This will be filled in by the binOp we call.
  IQLToLLVMValue::ValueType vt = IQLToLLVMValue::eLocal;
  if (n1 != NULL) {
    // The function we are working on.
    llvm::Function *f = b->GetInsertBlock()->getParent();
    // Create blocks for the then/value and else (likely to be next conditional).  
    // Insert the 'then/value' block at the end of the function.
    llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
    llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "cont", f);

    // Test if NULL and branch.  
    nv = n1;
    b->CreateCondBr(b->CreateNot(nv), thenBB, mergeBB);
    // Emit then value.
    b->SetInsertPoint(thenBB);  
    vt = (this->*unOp)(lhs, lhsType, result, resultType);
    b->CreateBr(mergeBB);    
    b->SetInsertPoint(mergeBB);
  } else {
    vt = (this->*unOp)(lhs, lhsType, result, resultType);
    result = trimAlloca(result, resultType);
  }
  // Return either pointer or value
  if (isPointerToValueType(result, resultType)) {
    result = b->CreateLoad(result);
  }
  return IQLToLLVMValue::get(this, result, nv, vt);
}

const IQLToLLVMValue * CodeGenerationContext::buildNonNullableUnaryOp(const IQLToLLVMValue * lhs, 
								      const FieldType * lhsType, 
								      const FieldType * resultType,
								      UnaryOperatorMemFn unOp)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // Allocate space for a result
  llvm::Type * retTy = resultType->LLVMGetType(this);
  llvm::Value * result = buildEntryBlockAlloca(retTy, "nonNullUnOp");    
  IQLToLLVMValue::ValueType vt = (this->*unOp)(lhs, lhsType, result, resultType);
  // Clean up the alloca since we really only needed it to call the cast
  // methods
  result = trimAlloca(result, resultType);
  // Return either pointer or value
  if (isPointerToValueType(result, resultType)) {
    result = b->CreateLoad(result);
  }
  // Just propagate the null value of the incoming arg
  return IQLToLLVMValue::get(this, result, lhs->getNull(), vt);
}

const IQLToLLVMValue * CodeGenerationContext::buildNullableBinaryOp(const IQLToLLVMValue * lhs, 
								    const FieldType * lhsType, 
								    const IQLToLLVMValue * rhs, 
								    const FieldType * rhsType, 
								    const FieldType * resultType,
								    BinaryOperatorMemFn binOp)
{
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * n1 = lhs->getNull();
  llvm::Value * n2 = rhs->getNull();
  BOOST_ASSERT((lhsType->isNullable() && n1 != NULL) ||
	       (!lhsType->isNullable() && n1 == NULL));
  BOOST_ASSERT((rhsType->isNullable() && n2 != NULL) ||
	       (!rhsType->isNullable() && n2 == NULL));
  if (lhs->isLiteralNull() || rhs->isLiteralNull()) {
    return buildNull();
  }
  llvm::Value * nv = NULL;
  // Allocate space for a result
  llvm::Type * retTy = resultType->LLVMGetType(this);
  llvm::Value * result = buildEntryBlockAlloca(retTy, "nullableBinOp");    
  // This will be filled in by the binOp we call.
  IQLToLLVMValue::ValueType vt = IQLToLLVMValue::eLocal;
  if (n1 != NULL || n2 != NULL) {
    // The function we are working on.
    llvm::Function *f = b->GetInsertBlock()->getParent();
    // Create blocks for the then/value and else (likely to be next conditional).  
    // Insert the 'then/value' block at the end of the function.
    llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
    llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "cont", f);

    // Test if NULL and branch.  We may have one or two values to check
    nv = n1 != NULL ? (n2 != NULL ? b->CreateOr(n1,n2) : n1) : n2;
    b->CreateCondBr(b->CreateNot(nv), thenBB, mergeBB);
    // Emit then value.
    b->SetInsertPoint(thenBB);  
    vt = (this->*binOp)(lhs, lhsType, rhs, rhsType, result, resultType);
    b->CreateBr(mergeBB);    
    b->SetInsertPoint(mergeBB);
  } else {
    vt = (this->*binOp)(lhs, lhsType, rhs, rhsType, result, resultType);
    result = trimAlloca(result, resultType);
  }
  // Return either pointer or value
  if (isPointerToValueType(result, resultType)) {
    result = b->CreateLoad(result);
  }
  return IQLToLLVMValue::get(this, result, nv, vt);
}

bool CodeGenerationContext::isChar(llvm::Type * ty)
{
  // This is safe for now since we don't have int8 as an 
  // IQL type.  Ultimately this should be removed and
  // we should be using FieldType for this info.
  llvm::PointerType * pTy = llvm::dyn_cast<llvm::PointerType>(ty);
  llvm::ArrayType * aTy = pTy != NULL ? llvm::dyn_cast<llvm::ArrayType>(pTy->getElementType()) : NULL;
  return aTy != NULL && aTy->getElementType()->isIntegerTy(8);
}
bool CodeGenerationContext::isChar(llvm::Value * val)
{
  return isChar(val->getType());
}

int32_t CodeGenerationContext::getCharArrayLength(llvm::Type * ty)
{
  return llvm::cast<llvm::ArrayType>(llvm::cast<llvm::SequentialType>(ty)->getElementType())->getNumElements();
}

int32_t CodeGenerationContext::getCharArrayLength(llvm::Value * val)
{
  return getCharArrayLength(val->getType());
}

llvm::Value * CodeGenerationContext::trimAlloca(llvm::Value * result, 
						const FieldType * resultType)
{
  if (isPointerToValueType(result, resultType)) {
    // Check whether the last instruction is a store to the result
    // Check whether that is the only use of result.  If so then
    // we can replace result by the value stored in it.  
    // The point here is to avoid too many alloca's from proliferating;
    // they will be removed by mem2reg during optimziation but
    // for really huge programs that takes a lot of time and memory.
    // It is much faster to fix up the problem here.
    llvm::AllocaInst * AI = 
      llvm::dyn_cast<llvm::AllocaInst>(result);
    if (AI != NULL && 
	AI->hasOneUse()) {
      if(llvm::StoreInst * SI = 
	 llvm::dyn_cast<llvm::StoreInst>(*AI->use_begin())) {
	if (!SI->isVolatile() && SI->getPointerOperand() == AI) {
	  // Conditions satisfied. Get rid of the store and the
	  // alloca and replace result with the value of the
	  // store.
	  llvm::Value * val = SI->getValueOperand();
	  SI->eraseFromParent();
	  AI->eraseFromParent();
	  return val;
	}
      }
    }
  }
  return result;
}

llvm::Value * CodeGenerationContext::getCachedLocal(llvm::Type * ty)
{
  local_cache::iterator it = AllocaCache->find(ty);
  if (it == AllocaCache->end() || 0==it->second.size()) return NULL;
  llvm::Value * v = it->second.back();
  it->second.pop_back();
  return v;
}

void CodeGenerationContext::returnCachedLocal(llvm::Value * v)
{
  const llvm::Type * ty = v->getType();
  const llvm::PointerType * pty = llvm::dyn_cast<llvm::PointerType>(ty);
  if (pty == NULL) return;
  ty = pty->getElementType();
  local_cache c(*AllocaCache);
  local_cache::iterator it = c.find(ty);
  if (it == c.end()) {
    c[ty] = std::vector<llvm::Value *>();
  }
  c[ty].push_back(v);
}

llvm::Value * CodeGenerationContext::buildEntryBlockAlloca(llvm::Type * ty, const char * name) {
  // Create a new builder positioned at the beginning of the entry block of the function
  llvm::Function* TheFunction = llvm::dyn_cast<llvm::Function>(llvm::unwrap(LLVMFunction));
  llvm::IRBuilder<> TmpB(&TheFunction->getEntryBlock(),
                 TheFunction->getEntryBlock().begin());
  return TmpB.CreateAlloca(ty, 0, name);
}

void CodeGenerationContext::buildSetValue2(const IQLToLLVMValue * iqlVal,
					   const IQLToLLVMValue * iqllvalue,
					   const FieldType * ft)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * lvalue = iqllvalue->getValue();
  if (NULL == lvalue)
    throw std::runtime_error("Undefined variable ");

  //
  // Perform any necessary type promotion of rvalue.
  //
  // TODO: Do we really need type promotion in this method at all anymore or has
  // it been handled by wrappers?
  // TODO: What are the interactions between type promotion and the
  // local/global value stuff going on in here?  Namely should we really
  // do type promotions first before handling global/local?
  // TODO: It really would have been easier to determine the type promotion
  // at type checking time when we had the IQL types handy.
  BOOST_ASSERT(llvm::Type::PointerTyID == lvalue->getType()->getTypeID());
  llvm::Value * llvmVal = iqlVal->getValue();

  // DECIMAL/VARCHAR expressions return a reference/pointer.  
  // Before setting we must load.  Perhaps we'd be better off with
  // a memcpy instrinsic here.
  if (ft->GetEnum() == FieldType::BIGDECIMAL ||
      ft->GetEnum() == FieldType::CHAR ||
      ft->GetEnum() == FieldType::FIXED_ARRAY) {
    // TODO: Should probably use memcpy rather than load/store
    llvmVal = b->CreateLoad(llvmVal);
  } else if (ft->GetEnum() == FieldType::VARCHAR) {
    // TODO:
    // Four cases here depending on the global/local dichotomy for the source
    // value and the target variable.
    // Source value global, Target Variable global : Must deep copy string (unless move semantics specified) remove from interpreter heap
    // Source value global, Target Variable local : Must deep copy string, must record in interpreter heap
    // Source value local, Target Variable global : May shallow copy but must remove source memory from interpreter heap.  TODO: This doesn't seem right!  What if two globals refer to the same local value?  Is this possible?  It may not be if the local is a temporary created for a string expression but it may be if we have a string variable on the stack.  It seems we need  use-def chain to know what is the right thing to do here.
    // Source value local, Target Variable local : May shallow copy string
    // Note that above rules depend critically on the assumption that strings are not
    // modifiable (because the above rules allow multiple local variables to 
    // reference the same underlying pointer).  
    // Also the above rules depend on the fact that
    // we know in all case where a value came from.  With mutable local variables it would
    // possible that the variable could have a local value at some point and a global value at
    // another. Removing this ambiguity is why global value assignments to local variables must
    // deep copy using the local heap.
    // Move semantics make all of this much more complicated and I don't think I
    // understand how to do this properly at this point.
    // For global values that contain pointers (e.g. strings)
    // we must copy the string onto the appropriate heap or at least disassociate from
    // internal heap tracking.  Here we take the latter path.  It is cheaper but less general
    // in that it assumes that the heap used by the IQL runtime is the same as that used
    // by the client of the runtime.
    if (iqlVal->getValueType() != IQLToLLVMValue::eLocal) {
      // TODO: IF we swap args 1,2 we should be able to use buildCall()
      // Call to copy the varchar before setting.
      llvm::Value * callArgs[4];
      llvm::Value * fn = llvm::unwrap(LLVMModule)->getFunction("InternalVarcharCopy");
      callArgs[0] = llvmVal;
      callArgs[1] = buildEntryBlockAlloca(llvm::unwrap(LLVMVarcharType), "");
      callArgs[2] = b->getInt32(iqllvalue->getValueType() == IQLToLLVMValue::eGlobal ? 0 : 1);
      callArgs[3] = b->CreateLoad(getContextArgumentRef());
      b->CreateCall(fn, llvm::makeArrayRef(&callArgs[0], 4), "");
      llvmVal = callArgs[1];
    } else if (iqlVal->getValueType() == IQLToLLVMValue::eLocal &&
	iqllvalue->getValueType() == IQLToLLVMValue::eGlobal) {
      llvm::Value * callArgs[2];
      llvm::Value * fn = llvm::unwrap(LLVMModule)->getFunction("InternalVarcharErase");
      callArgs[0] = llvmVal;
      callArgs[1] = b->CreateLoad(getContextArgumentRef());
      b->CreateCall(fn, llvm::makeArrayRef(&callArgs[0], 2), "");
    }
    // Load before store since we have a pointer to the Varchar.
    llvmVal = b->CreateLoad(llvmVal);    
  } 

  // Finally we can just issue the store.
  b->CreateStore(llvmVal, lvalue);
}

// set an rvalue into an lvalue handling nullability
// if allowNullToNonNull is false this throws if the 
// rvalue is nullable and the lvalue is not nullable.
// if true this is allowed.  It is incumbent on the caller
// to know if sufficient runtime checks have been performed
// in order for us to safely coerce a nullable value into
// a nonnullable one (e.g. IFNULL(x,y) is the canonical example).
void CodeGenerationContext::buildSetNullableValue(const IQLToLLVMLValue * lval,
						  const IQLToLLVMValue * val,
						  const FieldType * ft,
						  bool allowNullToNonNull)
{
  // Check nullability of value and target
  if (lval->isNullable()) {
    // Unwrap to C++
    llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
    llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
    if (val->isLiteralNull()) {
      // NULL literal
      lval->setNull(this, true);
    } else if (val->getNull() != NULL) {
      // Set or clear NULL bit.  Only set value if val is NOT NULL
      // Code we are generating is the following pseudocode:
      // if (!isNull(val)) {
      //   clearNull(member);
      //   setValue(member, val);
      // } else {
      //   setNull(member);
      //
    
      // The function we are working on.
      llvm::Function *f = b->GetInsertBlock()->getParent();
      // Create blocks for the then/value and else (likely to be next conditional).  
      // Insert the 'then/value' block at the end of the function.
      llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
      llvm::BasicBlock * elseBB = llvm::BasicBlock::Create(*c, "else", f);
      llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "cont", f);
      // Test if NULL and branch
      b->CreateCondBr(b->CreateNot(val->getNull()), 
		      thenBB, elseBB);
      // Emit then value.
      b->SetInsertPoint(thenBB);  
      lval->setNull(this, false);
      buildSetValue2 (val, lval->getValuePointer(this), ft);
      b->CreateBr(mergeBB);

      // Now the NULL case: here we just clear the NULL bit
      b->SetInsertPoint(elseBB);
      lval->setNull(this, true);
      b->CreateBr(mergeBB);
      b->SetInsertPoint(mergeBB);
    } else {
      // Setting non-nullable value into a nullable lvalue.
      buildSetValue2 (val, lval->getValuePointer(this), ft);
      lval->setNull(this, false);
    }
  } else {
    BOOST_ASSERT(allowNullToNonNull ||
		 val->getNull() == NULL);
    buildSetValue2 (val, lval->getValuePointer(this), ft);
  }
}

void CodeGenerationContext::buildSetNullableValue(const IQLToLLVMLValue * lval,
						  const IQLToLLVMValue * val,
						  const FieldType * valType,
						  const FieldType * lvalType)
{
  // TODO: Would it be more efficient to push the type promotion down because we
  // are checking nullability twice this way.
  const IQLToLLVMValue * cvt = buildCast(val, valType, lvalType);
  buildSetNullableValue(lval, cvt, lvalType, false);
}

void CodeGenerationContext::buildSetValue(const IQLToLLVMValue * iqlVal, const char * loc, const FieldType * ft)
{
  const IQLToLLVMLValue * lval = lookup(loc, NULL);
  buildSetNullableValue(lval, iqlVal, ft, false);
}

void CodeGenerationContext::buildCaseBlockBegin(const FieldType * caseType)
{
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Function *f = b->GetInsertBlock()->getParent();

  // Create a merge block with a PHI node 
  // Save the block and PHI so that incoming values may be added.
  // The block itself will be emitted at the end of the CASE expression.
  llvm::BasicBlock * mergeBB = llvm::BasicBlock::Create(*c, "casemerge", f);
  // Allocate space for a result.  Try to reuse space used for a
  // completed CASE.
  llvm::Type * retTy = caseType->LLVMGetType(this);
  llvm::Value * result = getCachedLocal(retTy);
  if (result == NULL) {
    result = buildEntryBlockAlloca(retTy, "caseResult");    
  }
  llvm::Value * nullVal = NULL;
  if (caseType->isNullable()) {
    nullVal = buildEntryBlockAlloca(b->getInt1Ty(), "caseNullBit");
  }     
  IQLToLLVMLocal * lVal = new IQLToLLVMLocal(IQLToLLVMValue::get(this, result, IQLToLLVMValue::eLocal), nullVal);
  IQLCase.push(new IQLToLLVMCaseState(lVal, mergeBB));  
  BOOST_ASSERT(IQLCase.size() != 0);
}

void CodeGenerationContext::buildCaseBlockIf(const IQLToLLVMValue * condVal)
{
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // The function we are working on.
  llvm::Function *f = b->GetInsertBlock()->getParent();
  // Create blocks for the then/value and else (likely to be next conditional).  
  // Insert the 'then/value' block at the end of the function.
  llvm::BasicBlock * thenBB = llvm::BasicBlock::Create(*c, "then", f);
  // Save else block so we can append it after the value is emitted 
  BOOST_ASSERT(IQLCase.size() != 0);
  BOOST_ASSERT(IQLCase.top()->ElseBB == NULL);
  IQLCase.top()->ElseBB = llvm::BasicBlock::Create(*c, "else", f);
  // Handle ternary logic here
  llvm::Value * nv = condVal->getNull();
  if (nv) {
    llvm::BasicBlock * notNullBB = llvm::BasicBlock::Create(*c, "notNull", f);
    b->CreateCondBr(b->CreateNot(nv), notNullBB, IQLCase.top()->ElseBB);
    b->SetInsertPoint(notNullBB);
  }
  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(condVal->getValue(),
					  b->getInt32(0),
					  "boolCast");
  // Branch and set block
  b->CreateCondBr(boolVal, thenBB, IQLCase.top()->ElseBB);
  // Emit then value.
  b->SetInsertPoint(thenBB);  
}

void CodeGenerationContext::buildCaseBlockThen(const IQLToLLVMValue *value, const FieldType * valueType, const FieldType * caseType, bool allowNullToNonNull)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);

  // Convert to required type 
  const IQLToLLVMValue * cvtVal = buildCast(value, valueType, caseType);
  if (NULL == cvtVal) {
    // This should always succeed as type checking should have
    // validated the conversion.
    llvm::Type * cvtTy = caseType->LLVMGetType(this);
    std::cerr << "INTERNAL ERROR: type promotion failed.\nTarget LLVM Type: ";
    cvtTy->dump();
    std::cerr << "\nSource LLVM Type: ";
    value->getValue()->getType()->dump();
    std::cerr << std::endl;
    throw std::runtime_error("INTERNAL ERROR: Failed type promotion during code generation");
  }
  BOOST_ASSERT(IQLCase.size() != 0);
  IQLToLLVMCaseState * state = IQLCase.top();
  // Store converted value in the case return variable
  buildSetNullableValue(state->Local, cvtVal, 
			caseType, allowNullToNonNull);
  // Branch to block with PHI
  b->CreateBr(state->getMergeBlock());
  
  // Emit else block if required (will be required
  // if we are WHEN but not for an ELSE).
  if (state->ElseBB) {
    b->SetInsertPoint(state->ElseBB);
    state->ElseBB = NULL;
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildCaseBlockFinish(const FieldType * caseType)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);

  // Emit merge block 
  BOOST_ASSERT(IQLCase.size() != 0);
  b->SetInsertPoint(IQLCase.top()->getMergeBlock());

  // This is our return value
  IQLToLLVMLocal * lVal = IQLCase.top()->Local;
  const IQLToLLVMValue * rVal = lVal->getEntirePointer(this);
  // Pointer to value we allocated
  llvm::Value *result=rVal->getValue();
  // Get the null bit if a nullable value
  llvm::Value * nullBit = rVal->getNull();
  // Return either pointer or value
  if (result != NULL &&
      isPointerToValueType(result, caseType)) {
    // The pointer to the local will not be used beyond this point,
    // so it may be reused.
    returnCachedLocal(result);
    result = b->CreateLoad(result);
  }
  // Done with this CASE so pop off.
  delete IQLCase.top();
  IQLCase.pop();

  return IQLToLLVMValue::get(this, result, nullBit, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildLeastGreatest(const std::vector<IQLToLLVMTypedValue> & args,
								 const FieldType * retTy,
								 bool isLeast)
{
  // Recursively generate Greatest(x1,x2,..., xN) = Greatest(Greatest(x1,x2),x3, ..., xN)
  // Greatest(x1,x2) = x1 < x2 ? x2 : x1
  // and similarly for least.
  if (args.size() == 2) {
    buildCaseBlockBegin(retTy);        
    // Hack!  Essentially we are implementing a tree rewrite during code generation and
    // unfortunately I need access to a type that would have been computed during semantic
    // analysis post-tree rewrite.  Create the correct type here.  This is dangerous since I
    // don't have access to the context in which other types were created so pointer comparison
    // of types won't work properly.  In addition I am breaking the abstraction of the type checker
    // and its representation of boolean types
    DynamicRecordContext tmpCtxt;
    Int32Type * tmpTy = Int32Type::Get(tmpCtxt, args[0].getType()->isNullable() || args[1].getType()->isNullable());
    buildCaseBlockIf(buildCompare(args[0].getValue(), 
				  args[0].getType(),
				  args[1].getValue(), 
				  args[1].getType(),
				  tmpTy,
				  isLeast ? IQLToLLVMOpLT : IQLToLLVMOpGT));
    buildCaseBlockThen(args[0].getValue(), args[0].getType(), retTy, false);
    buildCaseBlockThen(args[1].getValue(), args[1].getType(), retTy, false);
    return buildCaseBlockFinish(retTy);
  } else if (args.size() == 1) {
    // Convert type if necessary.
    return buildCast(args[0].getValue(), args[0].getType(), retTy);
  } else {
    BOOST_ASSERT(args.size() > 2);
    std::vector<IQLToLLVMTypedValue> a(args.begin(), args.begin()+2);
    std::vector<IQLToLLVMTypedValue> b(1, IQLToLLVMTypedValue(buildLeastGreatest(a, retTy, isLeast), retTy));
    b.insert(b.end(), args.begin()+2, args.end());
    return buildLeastGreatest(b, retTy, isLeast);
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildIsNullFunction(const std::vector<IQLToLLVMTypedValue> & args,
								  const FieldType * retTy)
{
  const IQLToLLVMValue * val = args[0].getValue();
  const IQLToLLVMValue * alt = args[1].getValue();
  if (val->getNull() == NULL) {
    // If val is not nullable just return it.
    return val;
  } else {
    buildCaseBlockBegin(retTy);        
    buildCaseBlockIf(buildIsNull(args[0].getValue()));
    buildCaseBlockThen(args[1].getValue(), args[1].getType(), retTy, false);
    buildCaseBlockThen(args[0].getValue(), args[0].getType(), retTy, true);
    return buildCaseBlockFinish(retTy); 
  }
}

void CodeGenerationContext::buildBeginAnd(const FieldType * retType)
{
  buildCaseBlockBegin(retType);
}

void CodeGenerationContext::buildAddAnd(const IQLToLLVMValue * lhs,
					const FieldType * lhsType,
					const FieldType * retType)
{
  buildCaseBlockIf(lhs);
}

const IQLToLLVMValue * CodeGenerationContext::buildAnd(const IQLToLLVMValue * rhs,
						       const FieldType * rhsType,
						       const FieldType * retType)
{
  buildCaseBlockThen(rhs, rhsType, retType, false);
  // HACK! I need the FieldType for boolean and I don't have another way of getting it
  DynamicRecordContext tmpCtxt;
  Int32Type * tmpTy = Int32Type::Get(tmpCtxt, false);
  buildCaseBlockThen(buildFalse(), tmpTy, retType, false); 
  return buildCaseBlockFinish(retType);
}

void CodeGenerationContext::buildBeginOr(const FieldType * retType)
{
  buildCaseBlockBegin(retType);
}

void CodeGenerationContext::buildAddOr(const IQLToLLVMValue * lhs,
				       const FieldType * lhsType,
				       const FieldType * retType)
{
  buildCaseBlockIf(lhs);
  // HACK! I need the FieldType for boolean and I don't have another way of getting it
  DynamicRecordContext tmpCtxt;
  Int32Type * tmpTy = Int32Type::Get(tmpCtxt, false);
  buildCaseBlockThen(buildTrue(), tmpTy, retType, false); 
}

const IQLToLLVMValue * CodeGenerationContext::buildOr(const IQLToLLVMValue * rhs,
						      const FieldType * rhsType,
						      const FieldType * retType)
{
  buildCaseBlockThen(rhs, rhsType, retType, false);
  return buildCaseBlockFinish(retType);
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildNot(const IQLToLLVMValue * lhs,
				const FieldType * lhsTy,
				llvm::Value * ret,
				const FieldType * retTy)
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * one = b->getInt32(1);
  llvm::Value * e1 = lhs->getValue();
  // TODO: Currently representing booleans as int32_t
  // Implement NOT as x+1 & 1
  llvm::Value * r = b->CreateAnd(b->CreateAdd(e1,one),one);
  b->CreateStore(r, ret);
  return IQLToLLVMValue::eLocal;
}

const IQLToLLVMValue * CodeGenerationContext::buildNot(const IQLToLLVMValue * e, 
						       const FieldType * argType, 
						       const FieldType * retType)
{
  return buildNullableUnaryOp(e, argType, retType, &CodeGenerationContext::buildNot);
}

const IQLToLLVMValue * CodeGenerationContext::buildIsNull(const IQLToLLVMValue * val)
{
  llvm::Value * nv = val->getNull();
  if (nv) {
    // Extend to 32 bit integer.
    return buildCompareResult(nv);
  } else {
    return buildTrue();
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildIsNull(const IQLToLLVMValue * lhs,
							  const FieldType * lhsType, 
							  const FieldType * retType, 
							  int isNotNull)
{
  llvm::Value * nv = lhs->getNull();
  if (nv) {
    // Nullable value.  Must check the null... 
    if (isNotNull) {
      llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
      nv = b->CreateNot(nv);
    }
    return buildCompareResult(nv);
  } else {
    return isNotNull ? buildTrue() : buildFalse();
  }
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildVarcharCompare(llvm::Value * e1, 
					   llvm::Value * e2,
					   llvm::Value * ret,
					   IQLToLLVMPredicate opCode)
{
  // TODO: Inline.  For now call an external function.
  const char * booleanFuncs [] = { "InternalVarcharEquals",
				   "InternalVarcharNE",
				   "InternalVarcharGT",
				   "InternalVarcharGE",
				   "InternalVarcharLT",
				   "InternalVarcharLE",
                                   "InternalVarcharRLike"
  };
  const char * tmpNames [] = { "varchareqtmp",
			       "varcharnetmp",
			       "varchargttmp",
			       "varchargetmp",
			       "varcharlttmp",
			       "varcharletmp",
                               "varcharrliketmp"
  };
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * callArgs[3];
  llvm::Value * fn = llvm::unwrap(LLVMModule)->getFunction(booleanFuncs[opCode]);
  callArgs[0] = e1;
  callArgs[1] = e2;
  callArgs[2] = b->CreateLoad(getContextArgumentRef());
  llvm::Value * cmp = b->CreateCall(fn, llvm::makeArrayRef(&callArgs[0], 3), tmpNames[opCode]);
  b->CreateStore(cmp, ret);
  return IQLToLLVMValue::eLocal;
}

IQLToLLVMValue::ValueType 
CodeGenerationContext::buildCompare(const IQLToLLVMValue * lhs, 
				    const FieldType * lhsType, 
				    const IQLToLLVMValue * rhs, 
				    const FieldType * rhsType,
				    llvm::Value * ret,
				    const FieldType * retType,
				    IQLToLLVMPredicate op)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);

  llvm::CmpInst::Predicate intOp = llvm::CmpInst::ICMP_EQ;
  llvm::CmpInst::Predicate realOp = llvm::CmpInst::FCMP_FALSE;
  switch(op) {
  case IQLToLLVMOpEQ:
    intOp = llvm::CmpInst::ICMP_EQ;
    realOp = llvm::CmpInst::FCMP_OEQ;
    break;
  case IQLToLLVMOpNE:
    intOp = llvm::CmpInst::ICMP_NE;
    realOp = llvm::CmpInst::FCMP_ONE;
    break;
  case IQLToLLVMOpGT:
    intOp = llvm::CmpInst::ICMP_SGT;
    realOp = llvm::CmpInst::FCMP_OGT;
    break;
  case IQLToLLVMOpGE:
    intOp = llvm::CmpInst::ICMP_SGE;
    realOp = llvm::CmpInst::FCMP_OGE;
    break;
  case IQLToLLVMOpLT:
    intOp = llvm::CmpInst::ICMP_SLT;
    realOp = llvm::CmpInst::FCMP_OLT;
    break;
  case IQLToLLVMOpLE:
    intOp = llvm::CmpInst::ICMP_SLE;
    realOp = llvm::CmpInst::FCMP_OLE;
    break;
  case IQLToLLVMOpRLike:
    // This will only happen with string types
    break;
  }
  // UGLY: I don't actually what type I should be promoting to since that is determined during type check
  // but not being stored in the syntax tree.  Recompute here.
  // TODO: Fix this by storing promoted type in syntax tree or rewriting the syntax tree with a cast.
  const FieldType * promoted = TypeCheckContext::leastCommonTypeNullable(lhsType, rhsType);
  lhs = buildCastNonNullable(lhs, lhsType, promoted);
  rhs = buildCastNonNullable(rhs, rhsType, promoted);
  
  // Call out to external function.  The trick is that we always pass a pointer to data.
  // Handle the cases here.  For value types, we must alloca storage so
  // we have a pointer to pass.
  llvm::Value * e1 = lhs->getValue();
  llvm::Value * e2 = rhs->getValue();

  // TODO: I should be able to reliably get the length of fixed size fields from the FieldType
  llvm::Value * r = NULL;
  if (promoted->isIntegral() || promoted->GetEnum() == FieldType::DATE || promoted->GetEnum() == FieldType::DATETIME) {
    return buildCompareResult(b->CreateICmp(intOp, e1, e2), ret);
  } else if (lhsType->isFloatingPoint() || rhsType->isFloatingPoint()) {
    // TODO: Should this be OEQ or UEQ?
    // TODO: Should be comparison within EPS?
    return buildCompareResult(b->CreateFCmp(realOp, e1, e2), ret);
  } else if (promoted->GetEnum() == FieldType::VARCHAR) {
    return buildVarcharCompare(e1, e2, ret, op);
  } else if (promoted->GetEnum() == FieldType::CHAR) {

    // TODO: I don't really know what the semantics of char(N) equality are (e.g. when the sizes aren't equal).
    // FWIW, semantics of char(N) equality is that one compares assuming
    // space padding to MAX(M,N)
    // For now I am draconian and say they are never equal????  
    if (getCharArrayLength(e1) != getCharArrayLength(e2)) {
      r = b->getInt32(0);
      return IQLToLLVMValue::eLocal;
    } else {
      llvm::Value * m = buildMemcmp(e1, FieldAddress(), e2, FieldAddress(), getCharArrayLength(e1));
      // Compare result to zero and return
      return buildCompareResult(b->CreateICmp(intOp, b->getInt32(0),m),
				ret);
    }
  } else if (promoted->GetEnum() == FieldType::BIGDECIMAL) {
    // Call decNumberCompare
    llvm::Value * callArgs[4];
    llvm::Value * fn = llvm::unwrap(LLVMModule)->getFunction("InternalDecimalCmp");
    callArgs[0] = e1;
    callArgs[1] = e2;
    callArgs[2] = buildEntryBlockAlloca(b->getInt32Ty(), "decimalCmpRetPtr");
    callArgs[3] = b->CreateLoad(getContextArgumentRef());
    b->CreateCall(fn, llvm::makeArrayRef(&callArgs[0], 4), "");
    
    // Compare result to zero and return
    return buildCompareResult(b->CreateICmp(intOp, b->getInt32(0), b->CreateLoad(callArgs[2])),
			      ret);
  } else {
    throw std::runtime_error("CodeGenerationContext::buildCompare unexpected type");
  }
  // Copy result into provided storage.
  b->CreateStore(r, ret);
  return IQLToLLVMValue::eLocal;
}

const IQLToLLVMValue * CodeGenerationContext::buildCompare(const IQLToLLVMValue * lhs, 
							   const FieldType * lhsType, 
							   const IQLToLLVMValue * rhs, 
							   const FieldType * rhsType,
							   const FieldType * resultType,
							   IQLToLLVMPredicate op)
{
  BinaryOperatorMemFn opFun=NULL;
  switch(op) {
  case IQLToLLVMOpEQ:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpEQ>;
    break;
  case IQLToLLVMOpNE:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpNE>;
    break;
  case IQLToLLVMOpGT:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpGT>;
    break;
  case IQLToLLVMOpGE:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpGE>;
    break;
  case IQLToLLVMOpLT:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpLT>;
    break;
  case IQLToLLVMOpLE:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpLE>;
    break;
  case IQLToLLVMOpRLike:
    opFun = &CodeGenerationContext::buildCompare<IQLToLLVMOpRLike>;
    break;
  default:
    throw std::runtime_error("Unexpected predicate type");
  }
  return buildNullableBinaryOp(lhs, lhsType, rhs, rhsType, resultType, opFun);
}

const IQLToLLVMValue * CodeGenerationContext::buildHash(const std::vector<IQLToLLVMTypedValue> & args)
{
  // Call out to external function.  The trick is that we always pass a pointer to data.
  // Handle the cases here.  For value types, we must alloca storage so
  // we have a pointer to pass.
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * callArgs[3];
  llvm::Value * fn = llvm::unwrap(LLVMModule)->getFunction("SuperFastHash");

  // Save the previous hash so we can feed it into the next.  
  llvm::Value * previousHash=NULL;
  for(std::size_t i=0; i<args.size(); i++) {
    llvm::Value * argVal = args[i].getValue()->getValue();
    llvm::Type * argTy = llvm::PointerType::get(b->getInt8Ty(), 0);
    // TODO: Not handling NULL values in a coherent way.
    // TODO: I should be able to reliably get the length of fixed size fields from the LLVM type.
    if (argVal->getType() == b->getInt32Ty()) {
      llvm::Value * tmpVal = buildEntryBlockAlloca(argVal->getType(), "hash32tmp");
      b->CreateStore(argVal, tmpVal);
      callArgs[0] = b->CreateBitCast(tmpVal, argTy);
      callArgs[1] = b->getInt32(4);
      callArgs[2] = i==0 ? b->getInt32(4) : previousHash;
    } else if (argVal->getType() == b->getInt64Ty() ||
	       argVal->getType() == b->getDoubleTy()) {
      llvm::Value * tmpVal = buildEntryBlockAlloca(argVal->getType(), "hash64tmp");
      b->CreateStore(argVal, tmpVal);
      callArgs[0] = b->CreateBitCast(tmpVal, argTy);
      callArgs[1] = b->getInt32(8);
      callArgs[2] = i==0 ? b->getInt32(8) : previousHash;
    } else if (args[i].getType()->GetEnum() == FieldType::VARCHAR) {
      callArgs[0] = buildVarcharGetPtr(argVal);
      callArgs[1] = buildVarcharGetSize(argVal);
      callArgs[2] = i==0 ? callArgs[1] : previousHash;
    } else if (isChar(argVal)) {
      // Hash on array length - 1 for CHAR types because of trailing null char and we want
      // consistency with varchar hashing.
      unsigned arrayLen = getCharArrayLength(argVal);
      if (args[i].getType()->GetEnum() == FieldType::CHAR) {
	arrayLen -= 1;
      }
      callArgs[0] = b->CreateBitCast(argVal, argTy);
      callArgs[1] = b->getInt32(arrayLen);
      callArgs[2] = i==0 ? b->getInt32(arrayLen) : previousHash;    
    } else if (args[i].getType()->GetEnum() == FieldType::BIGDECIMAL) {
      callArgs[0] = b->CreateBitCast(argVal, argTy);
      callArgs[1] = b->getInt32(16);
      callArgs[2] = i==0 ? b->getInt32(16) : previousHash;
    } else {
      throw std::runtime_error("CodeGenerationContext::buildHash unexpected type");
    }
    previousHash = b->CreateCall(fn, llvm::makeArrayRef(&callArgs[0], 3), "hash");
  }

  return IQLToLLVMValue::get(this, previousHash, IQLToLLVMValue::eLocal);
}

// TODO: Handle all types.
// TODO: Handle case of char[N] for N < 4 and BOOLEAN which don't
// fully utilize the prefix.
// TODO: Use FieldType as necessary....
const IQLToLLVMValue * CodeGenerationContext::buildSortPrefix(const IQLToLLVMValue * arg, 
							      const FieldType * argTy)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * argVal = arg->getValue();
  llvm::Value * retVal = NULL;
  // 31 bits for the prefix
  static const int32_t prefixBits = 31;
  if (argVal->getType() == b->getInt32Ty()) {
    // For signed integer quantity, reverse top bit then shift down to get 31 bits
    // of precision.
    llvm::Value * offsetVal = 
      b->CreateXor(argVal, b->getInt32(0x80000000), "int32PrefixOffset");
    retVal = b->CreateLShr(offsetVal, b->getInt32(32-prefixBits), "int32Prefix");
  } else if (argVal->getType() == b->getInt64Ty()) {
    // For signed integer quantity, reverse top bit then shift down to get 31 bits
    // of precision.
    llvm::Value * offsetVal = 
      b->CreateXor(argVal, b->getInt64(0x8000000000000000LL), "int64PrefixOffset");
    retVal = b->CreateLShr(offsetVal, b->getInt64(64-prefixBits), "int64PrefixAs64");
    retVal = b->CreateTrunc(retVal, b->getInt32Ty(), "int64Prefix");
  } else if (isChar(argVal)) {
    // Take top 32 bits of array, byte swap into a uint32_t and then divide by 2
    // TODO: If there are less than 32 bits in the array then there is room
    // for parts of other field(s) in the prefix...
    retVal = b->getInt32(0);
    int numBytes = getCharArrayLength(argVal) >= 4 ? 
      4 : (int) getCharArrayLength(argVal);
    for(int i = 0; i<numBytes; ++i) {
      llvm::Value * tmp = b->CreateLoad(b->CreateConstInBoundsGEP2_64(argVal, 0, i));
      tmp = b->CreateShl(b->CreateZExt(tmp, b->getInt32Ty()),
			 b->getInt32(8*(3-i)));
      retVal = b->CreateOr(retVal, tmp, "a");
    }
    retVal = b->CreateLShr(retVal, b->getInt32(32-prefixBits));
  } else {
    // TODO: This is suboptimal but allows sorting to work.
    // TODO: Handle other types...
    retVal = b->getInt32(1);
  }

  return IQLToLLVMValue::get(this, retVal, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildSortPrefix(const std::vector<IQLToLLVMTypedValue> & args,
							      const FieldType * retTy)
{
  // TODO: Assume for the moment that one field generates
  // the entire prefix.
  const IQLToLLVMValue * arg = args[0].getValue();
  const FieldType * argTy = args[0].getType();
  if (arg->getNull()) {
    const IQLToLLVMValue * zero = 
      IQLToLLVMValue::get(this, 
			  retTy->getZero(this),
			  IQLToLLVMValue::eLocal);
    buildCaseBlockBegin(retTy);
    buildCaseBlockIf(buildIsNull(arg));
    buildCaseBlockThen(zero, retTy, retTy, false);
    const IQLToLLVMValue * p = buildSortPrefix(arg, argTy);
    buildCaseBlockThen(p, retTy, retTy, false);
    return buildCaseBlockFinish(retTy);
  } else {
    return buildSortPrefix(arg, argTy);
  }
}

void CodeGenerationContext::buildReturnValue(const IQLToLLVMValue * iqlVal, const FieldType * retType)
{
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // Due to our uniform implementation of mutable variables, our return location is actually
  // a pointer to a pointer.
  llvm::Value * loc = b->CreateLoad(lookupValue("__ReturnValue__", NULL)->getValue());

  llvm::Value * llvmVal = iqlVal->getValue();

  llvm::BasicBlock * mergeBB = NULL;
  llvm::Value * nv = iqlVal->getNull();
  if (nv) {
    // We are only prepared to deal with NULLABLE int right now
    // which in fact is only return boolean values.  For backward
    // compatibility we return 0 for NULL.
    llvm::Function *f = b->GetInsertBlock()->getParent();
    if (llvmVal->getType() != b->getInt32Ty()) {
      throw std::runtime_error("Only supporting return of nullable values for INTEGERs");
    }
    llvm::BasicBlock * nullBB = llvm::BasicBlock::Create(*c, "retNullBlock", f);
    llvm::BasicBlock * notNullBB = llvm::BasicBlock::Create(*c, "retNotNullBlock", f);
    mergeBB = llvm::BasicBlock::Create(*c, "retMergeBlock", f);
    b->CreateCondBr(b->CreateNot(nv), notNullBB, nullBB);
    b->SetInsertPoint(nullBB);
    b->CreateStore(b->getInt32(0), loc);
    b->CreateBr(mergeBB);
    b->SetInsertPoint(notNullBB);
  }

  // Expressions are either by value or by reference.  For reference types
  // we load before store (maybe we should use the memcpy intrinsic instead).
  if (isValueType(retType)) {
    b->CreateStore(llvmVal, loc); 
  } else {
    llvm::Value * val = b->CreateLoad(llvmVal);
    llvm::SequentialType * st = llvm::dyn_cast<llvm::SequentialType>(loc->getType());
    if (st == NULL || st->getElementType() != val->getType()) {
      loc->getType()->dump();
      val->getType()->dump();
      throw std::runtime_error("Type mismatch with return value");
    }
    b->CreateStore(val, loc);
  }
  // The caller expects a BB into which it can insert LLVMBuildRetVoid.
  if (mergeBB) {
    llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
    b->CreateBr(mergeBB);
    b->SetInsertPoint(mergeBB);
  }
}

void CodeGenerationContext::buildBeginIfThenElse(const IQLToLLVMValue * condVal)
{
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Create blocks for the then and else cases.  Insert the 'then' block at the
  // end of the function.
  IQLStack.push(new IQLToLLVMStackRecord());
  IQLStack.top()->ThenBB = llvm::BasicBlock::Create(*c, "then", TheFunction);
  IQLStack.top()->ElseBB = llvm::BasicBlock::Create(*c, "else");
  IQLStack.top()->MergeBB = llvm::BasicBlock::Create(*c, "ifcont");

  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(condVal->getValue(),
					  b->getInt32(0),
					  "boolCast");

  // Branch and set block
  b->CreateCondBr(boolVal, IQLStack.top()->ThenBB, IQLStack.top()->ElseBB);
  // Emit then value.
  b->SetInsertPoint(IQLStack.top()->ThenBB);  
}

void CodeGenerationContext::buildElseIfThenElse()
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  // We saved block.
  b->CreateBr(IQLStack.top()->MergeBB);
  // Codegen of 'Then' can change the current block, update ThenBB for the PHI.
  IQLStack.top()->ThenBB = b->GetInsertBlock();
  
  // Emit else block.
  TheFunction->getBasicBlockList().push_back(IQLStack.top()->ElseBB);
  b->SetInsertPoint(IQLStack.top()->ElseBB);
}

const IQLToLLVMValue * CodeGenerationContext::buildEndIfThenElse(const IQLToLLVMValue * thenVal, const FieldType * thenTy,
								 const IQLToLLVMValue * elseVal, const FieldType * elseTy,
								 const FieldType * retTy)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  b->CreateBr(IQLStack.top()->MergeBB);
  // Codegen of 'Else' can change the current block, update ElseBB for the PHI.
  IQLStack.top()->ElseBB = b->GetInsertBlock();
  
  // It is possible for the branches to differ with respect to their
  // locality.  If one side is local then we have to make a local
  // copy a global value.
  if (thenVal->getValueType() != elseVal->getValueType()) {
    // TODO: Handle this case by making a local copy of a global.
    throw std::runtime_error("Internal Error: If then else with a mix of local and global values not handled");
  }


  // Emit merge block.
  TheFunction->getBasicBlockList().push_back(IQLStack.top()->MergeBB);
  b->SetInsertPoint(IQLStack.top()->MergeBB);

  // Perform conversion in the merge block
  thenVal = buildCast(thenVal, thenTy, retTy);
  elseVal = buildCast(elseVal, elseTy, retTy);
  llvm::Value * e1 = thenVal->getValue();
  llvm::Value * e2 = elseVal->getValue();

  llvm::PHINode *PN = b->CreatePHI(retTy->LLVMGetType(this), 2, "iftmp");

  PN->addIncoming(e1, IQLStack.top()->ThenBB);
  PN->addIncoming(e2, IQLStack.top()->ElseBB);

  // Done with these blocks. Pop the stack of blocks.
  delete IQLStack.top();
  IQLStack.pop();

  return IQLToLLVMValue::get(this,
			     PN,
			     thenVal->getValueType());
}

void CodeGenerationContext::buildBeginSwitch()
{
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);

  // Create a new builder for switch
  IQLSwitch.push(new IQLToLLVMSwitchRecord());
  IQLSwitch.top()->Top = b->GetInsertBlock();
  IQLSwitch.top()->Exit = llvm::BasicBlock::Create(*c, "switchEnd");
}

void CodeGenerationContext::buildEndSwitch(const IQLToLLVMValue * switchExpr)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * v = switchExpr->getValue();

  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  // Get switch builder and pop.
  IQLToLLVMSwitchRecord * s = IQLSwitch.top();
  IQLSwitch.pop();
  
  // Add the switch statement and cases.
  llvm::SwitchInst * si = llvm::SwitchInst::Create(v,
						   s->Exit, 
						   s->Cases.size(),
						   s->Top);
  typedef std::vector<std::pair<llvm::ConstantInt*, 
    llvm::BasicBlock*> > CaseVec;
  for(CaseVec::iterator it = s->Cases.begin();
      it != s->Cases.end();
      ++it) {
    si->addCase(it->first, it->second);
  }

  // Set builder to exit block and declare victory.
  TheFunction->getBasicBlockList().push_back(s->Exit);
  b->SetInsertPoint(s->Exit);

  delete s;
}

void CodeGenerationContext::buildBeginSwitchCase(const char * caseVal)
{
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);

  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();

  // Add block and set as insertion point
  llvm::BasicBlock * bb = llvm::BasicBlock::Create(*c, "switchCase");;
  llvm::ConstantInt * llvmConst = llvm::ConstantInt::get(b->getInt32Ty(),
							 caseVal,
							 10);
  IQLSwitch.top()->Cases.push_back(std::make_pair(llvmConst, bb));

  TheFunction->getBasicBlockList().push_back(bb);
  b->SetInsertPoint(bb);
}

void CodeGenerationContext::buildEndSwitchCase()
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // Get switch builder for exit block
  IQLToLLVMSwitchRecord * s = IQLSwitch.top();
  // Unconditional branch to exit block (implicit break).
  b->CreateBr(s->Exit);
}

const IQLToLLVMValue * CodeGenerationContext::buildInterval(const char * intervalType,
							    const  IQLToLLVMValue * e)
{
  // Right now we are storing as INT32 and type checking is enforcing that.
  return e;
}

const IQLToLLVMValue * CodeGenerationContext::buildDateLiteral(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // Strip quotes
  std::string str(val);
  str = str.substr(1, str.size()-2);
  boost::gregorian::date d = boost::gregorian::from_string(str);
  BOOST_STATIC_ASSERT(sizeof(boost::gregorian::date) == sizeof(int32_t));
  int32_t int32Date = *reinterpret_cast<int32_t *>(&d);

  return IQLToLLVMValue::get(this, 
			     b->getInt32(int32Date),
			     IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildDatetimeLiteral(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // Strip quotes
  std::string str(val);
  str = str.substr(1, str.size()-2);
  BOOST_STATIC_ASSERT(sizeof(boost::posix_time::ptime) == sizeof(int64_t));
  int64_t int64Date = 0;
  if (str.size() == 10) {
    boost::posix_time::ptime t(boost::gregorian::from_string(str));
    int64Date = *reinterpret_cast<int64_t *>(&t);
  } else {
    boost::posix_time::ptime t = boost::posix_time::time_from_string(str);
    int64Date = *reinterpret_cast<int64_t *>(&t);
  }
  return IQLToLLVMValue::get(this, 
			     b->getInt64(int64Date),
			     IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildDecimalInt32Literal(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  return IQLToLLVMValue::get(this, 
			     llvm::ConstantInt::get(b->getInt32Ty(), llvm::StringRef(val), 10),
			     IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildDecimalInt64Literal(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // Peel off the LL suffix
  std::string lit(val);
  lit = lit.substr(0, lit.size()-2);
  return IQLToLLVMValue::get(this, 
			     llvm::ConstantInt::get(b->getInt64Ty(), llvm::StringRef(lit.c_str()), 10), 
			     IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildDoubleLiteral(const char * val)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  return IQLToLLVMValue::get(this, 
			     llvm::ConstantFP::get(b->getDoubleTy(), llvm::StringRef(val)), 
			     IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildVarcharLiteral(const char * val)
{
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  // Strip off quotes
  // TODO: Proper unquotification
  int32_t len = strlen(val);
  BOOST_ASSERT(len >= 2);
  
  std::string str(val);
  str = str.substr(1, str.size()-2);
  // Unescape stuff
  boost::replace_all(str, "\\\\", "\\");
  boost::replace_all(str, "\\b", "\b");
  boost::replace_all(str, "\\t", "\t");
  boost::replace_all(str, "\\n", "\n");
  boost::replace_all(str, "\\f", "\f");
  boost::replace_all(str, "\\r", "\r");
  boost::replace_all(str, "\\'", "'");

  llvm::Type * int8Ty = b->getInt8Ty();
  if (str.size() < Varchar::MIN_LARGE_STRING_SIZE) {
    // Create type for small varchar
    llvm::Type * varcharMembers[2];
    varcharMembers[0] = int8Ty;
    varcharMembers[1] = llvm::ArrayType::get(int8Ty,
					     sizeof(VarcharLarge) - 1);
    llvm::Type * smallVarcharTy =
      llvm::StructType::get(*c, llvm::makeArrayRef(&varcharMembers[0], 2), false);
    // Create the global
    llvm::GlobalVariable * globalVar = new llvm::GlobalVariable(*llvm::unwrap(LLVMModule), 
								smallVarcharTy,
								false, llvm::GlobalValue::ExternalLinkage, 0, "smallVarcharLiteral");    
    // Initialize it
    // TODO: Encapsulate magic computation here of 2*sz which puts
    // the size into upper 7 bits and 0 into low bit.
    std::size_t sz = 2*str.size();
    llvm::Constant * constMembers[2];
    constMembers[0] = llvm::ConstantInt::get(int8Ty, sz, true);
    llvm::Constant * arrayMembers[sizeof(VarcharLarge)-1];
    for(std::size_t i=0; i<str.size(); i++) {
      arrayMembers[i] = llvm::ConstantInt::get(int8Ty, str[i], true);
    }
    for(std::size_t i=str.size(); i < sizeof(VarcharLarge)-1; i++) {
      arrayMembers[i] = llvm::ConstantInt::get(int8Ty, 0, true);
    }
    constMembers[1] = 
      llvm::ConstantArray::get(llvm::ArrayType::get(int8Ty, sizeof(VarcharLarge) - 1), 
			       llvm::makeArrayRef(&arrayMembers[0], sizeof(VarcharLarge)-1));
    llvm::Constant * globalVal = llvm::ConstantStruct::getAnon(*c, llvm::makeArrayRef(&constMembers[0], 2), true);
    globalVar->setInitializer(globalVal);
    // Cast to varchar type
    llvm::Value * val = 
      llvm::ConstantExpr::getBitCast(globalVar, llvm::PointerType::get(llvm::unwrap(LLVMVarcharType), 0));
    return IQLToLLVMValue::get(this, val, IQLToLLVMValue::eLocal);
  } else {
    // TODO: Remember the string is stored as a global so that we don't create it multiple times...
    // Put the string in as a global variable and then create a struct that references it.  
    // This is the global variable itself
    llvm::GlobalVariable * globalVar = new llvm::GlobalVariable(*llvm::unwrap(LLVMModule), 
								llvm::ArrayType::get(int8Ty, str.size() + 1),
								false, llvm::GlobalValue::ExternalLinkage, 0, "str");
    // This is the string value.  Set it as an initializer.
    llvm::Constant * constStr = llvm::ConstantDataArray::getString(*c,
								   llvm::StringRef(str.c_str(), str.size()),
								   false);
    globalVar->setInitializer(constStr);
    
    // Now to reference the global we have to use a const GEP.  To pass
    // by value into the copy method we have to create a stack variable
    // in order to get an address.
    uint32_t sz = (uint32_t) str.size();
    // Flip the bit to make this a Large model string.
    sz = 2*sz + 1;
    llvm::Constant * constStructMembers[3];
    constStructMembers[0] = llvm::ConstantInt::get(b->getInt32Ty(), sz, true);
    // Padding for alignment
    constStructMembers[1] = llvm::ConstantInt::get(b->getInt32Ty(), 0, true);
    llvm::Constant * constGEPIndexes[2];
    constGEPIndexes[0] = llvm::ConstantInt::get(b->getInt64Ty(), llvm::StringRef("0"), 10);
    constGEPIndexes[1] = llvm::ConstantInt::get(b->getInt64Ty(), llvm::StringRef("0"), 10);
    // The pointer to string
    constStructMembers[2] = llvm::ConstantExpr::getGetElementPtr(globalVar, llvm::makeArrayRef(&constGEPIndexes[0], 2));
    llvm::Constant * globalString = llvm::ConstantStruct::getAnon(*c, llvm::makeArrayRef(&constStructMembers[0], 3), false);
    llvm::Value * globalStringAddr = buildEntryBlockAlloca(llvm::unwrap(LLVMVarcharType), "globalliteral");
    b->CreateStore(globalString, globalStringAddr);
    
    return IQLToLLVMValue::get(this, globalStringAddr, IQLToLLVMValue::eGlobal);
  }
}

const IQLToLLVMValue * CodeGenerationContext::buildDecimalLiteral(const char * val)
{
  llvm::LLVMContext * c = llvm::unwrap(LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  decimal128 dec;
  decContext decCtxt;
  decContextDefault(&decCtxt, DEC_INIT_DECIMAL128); // no traps, please
  decimal128FromString(&dec, val, &decCtxt);

  // This is the global variable itself
  llvm::GlobalVariable * globalVar = new llvm::GlobalVariable(*llvm::unwrap(LLVMModule), 
						    llvm::unwrap(LLVMDecimal128Type),
						    false, llvm::GlobalValue::ExternalLinkage, 0, val);
  // The value to initialize the global
  llvm::Constant * constStructMembers[4];
  for(int i=0 ; i<4; i++) {
    constStructMembers[i] = llvm::ConstantInt::get(b->getInt32Ty(), ((int32_t *) &dec)[i], true);
  }
  llvm::Constant * globalVal = llvm::ConstantStruct::getAnon(*c, llvm::makeArrayRef(&constStructMembers[0], 4), true);
  globalVar->setInitializer(globalVal);

  return IQLToLLVMValue::get(this, 
			     globalVar,
			     IQLToLLVMValue::eLocal); 
}

const IQLToLLVMValue * CodeGenerationContext::buildTrue()
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * t = b->getInt32(1);
  return IQLToLLVMValue::get(this, t, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildFalse()
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * t = b->getInt32(0);
  return IQLToLLVMValue::get(this, t, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildNull()
{
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  llvm::Value * t = b->getInt1(0);
  llvm::Value * tmp = NULL;
  return IQLToLLVMValue::get(this, tmp, t, IQLToLLVMValue::eLocal);
}

const IQLToLLVMValue * CodeGenerationContext::buildLiteralCast(const char * val,
							       const char * typeName)
{
  if (boost::algorithm::iequals("date", typeName)) {
    return buildDateLiteral(val);
  } else {
    throw std::runtime_error((boost::format("Invalid type: %1%") %
			      typeName).str());
  }  
}

const IQLToLLVMValue * CodeGenerationContext::buildAggregateFunction(const char * fn,
								     const IQLToLLVMValue * e,
								     const FieldType * retTy)
{
  // Name of variable of this function
  std::string aggFn = (boost::format("__AggFn%1%__") % AggFn).str();
  // Take the return value and increment the aggregate variable
  // in the update context

  // TODO: Get the aggregate function instance
  // from the symbol table.
  boost::shared_ptr<AggregateFunction> agg = AggregateFunction::get(fn);
  agg->update(this, aggFn, e, retTy);
  // Move temporarily to the initialization context and provide init
  // for the aggregate variable
  restoreAggregateContext(&Initialize);

  int saveIsIdentity = IsIdentity;
  buildSetField(&AggFn, agg->initialize(this, retTy));
  IsIdentity = saveIsIdentity;

  // Shift back to transfer context and return a reference to the
  // aggregate variable corresponding to this aggregate function.
  restoreAggregateContext(&Transfer);

  return buildVariableRef(aggFn.c_str(), NULL, retTy);
}

void CodeGenerationContext::buildSetField(int * pos, const IQLToLLVMValue * val)
{
  const RecordType * outputRecord = unwrap(IQLOutputRecord);
  std::string memberName = outputRecord->GetMember(*pos).GetName();
  FieldAddress outputAddress = outputRecord->getFieldAddress(memberName);
  const FieldType * fieldType = outputRecord->GetMember(*pos).GetType();
  IQLToLLVMField fieldLVal(this, outputRecord, memberName, "__OutputPointer__");

  *pos += 1;

  // Track whether this transfer is an identity or not.
  std::map<std::string, std::pair<std::string, const RecordType *> >& recordTypes(*unwrap(IQLRecordArguments));
  if (recordTypes.size() == 1 && 
      recordTypes.begin()->second.second->size() == outputRecord->size()) {
    // TODO: Verify that the types are structurally the same.
    const std::string& inputArg(recordTypes.begin()->second.first);
    const RecordType * inputType = recordTypes.begin()->second.second;
    // Now check whether we are copying a field directly from input
    // to output at exactly the same offset/address
    llvm::Value * llvmVal = val->getValue();
    if (NULL == llvmVal) {
      IsIdentity = false;
    } else {
      if (llvm::LoadInst * load = llvm::dyn_cast<llvm::LoadInst>(llvmVal)) {
	llvmVal = load->getOperand(0);
      }
      FieldAddress inputAddress;
      llvm::Value * inputBase = lookupBasePointer(inputArg.c_str())->getValue();
      if (!inputType->isMemberPointer(llvmVal, inputBase, inputAddress) ||
	  inputAddress != outputAddress) {    
	IsIdentity = false;
      }
    }
  } else {
    IsIdentity = false;
  }

  buildSetNullableValue(&fieldLVal, val, fieldType, false);
}

void CodeGenerationContext::buildSetFields(const char * recordName, int * pos)
{
  // Unwrap to C++
  llvm::IRBuilder<> * b = llvm::unwrap(LLVMBuilder);
  typedef std::map<std::string, std::pair<std::string, const RecordType *> > RecordArgs;
  // Copy all fields from the source record to the output.
  const RecordArgs& recordTypes(*unwrap(IQLRecordArguments));
  // Find the record struct in the named inputs.
  RecordArgs::const_iterator it = recordTypes.find(recordName);
  if (it == recordTypes.end()) {
    std::string namedInputs;
    for(RecordArgs::const_iterator argIt = recordTypes.begin();
	argIt != recordTypes.end();
	++argIt) {
      if (namedInputs.size()) namedInputs += ", ";
      namedInputs += argIt->first;
    }
    throw std::runtime_error((boost::format("Undefined input record: %1%."
					    " Defined records: %2%.") % 
			      recordName %
			      namedInputs).str());
  }
  // If move semantics specified then we can just reduce this to a
  // couple of memcpy's and memset's.  Replace all source fields in the symbol table with
  // corresponding targets.
  if(IQLMoveSemantics) {
    RecordTypeMove mv(it->second.second, unwrap(IQLOutputRecord));
    for(std::vector<MemcpyOp>::const_iterator opit = mv.getMemcpy().begin();
	opit != mv.getMemcpy().end();
	++opit) {
      // TODO: How do I actually know what input argument these are bound to?  It should be resolved
      // by the above query to IQLInputRecords.
      buildMemcpy(it->second.first,
			  opit->mSourceOffset,
			  "__OutputPointer__",
			  opit->mTargetOffset,
			  opit->mSize);
    }
    for(std::vector<MemsetOp>::const_iterator opit = mv.getMemset().begin();
	opit != mv.getMemset().end();
	++opit) {
      buildMemset(b->CreateLoad(lookupBasePointer(it->second.first.c_str())->getValue()),
			  opit->mSourceOffset,
			  0,
			  opit->mSize);
    }
    
    // For each input member moved, get address of the corresponding field in
    // the target but don't put into symbol table.  
    // TODO: Any expressions that reference these values will be broken now!
    // TODO: If the target does not have the same name as the source then
    // we have a bad situation since the source has now been cleared.
    // It is probably better to create a new basic block at the end of
    // the function and put the memset stuff there.
    for(RecordType::const_member_iterator mit = it->second.second->begin_members();
	mit != it->second.second->end_members();
	++mit) {
      const RecordType * out = unwrap(IQLOutputRecord);
      std::string memberName = out->GetMember(*pos).GetName();
      out->LLVMMemberGetPointer(memberName, 
				this, 
				lookupBasePointer("__OutputPointer__")->getValue(),
				false
				);

      *pos += 1;
    }
  } else {    
    buildSetFieldsRegex(it->second.first,
			it->second.second, ".*", "", recordName, pos);
  }
}

void CodeGenerationContext::buildQuotedId(const char * quotedId, const char * rename, int * pos)
{
  // Strip off backticks.
  std::string strExpr(quotedId);
  strExpr = strExpr.substr(1, strExpr.size() - 2);
  std::string renameExpr(rename ? rename : "``");
  renameExpr = renameExpr.substr(1, renameExpr.size() - 2);
  typedef std::map<std::string, std::pair<std::string, const RecordType *> > RecordArgs;
  const RecordArgs& recordTypes(*unwrap(IQLRecordArguments));
  for(RecordArgs::const_iterator it = recordTypes.begin();
      it != recordTypes.end();
      ++it) {
    buildSetFieldsRegex(it->second.first,
			it->second.second, strExpr, renameExpr, "", pos);
  }
}

class NullInitializedAggregate : public AggregateFunction
{
protected:
  virtual void updateNull(CodeGenerationContext * ctxt,
			  const std::string& aggFn,
			  const IQLToLLVMValue * inc,
			  const FieldType * inputTy,
			  const IQLToLLVMLValue * fieldLVal,
			  const FieldType * ft)=0;
  virtual void updateNotNull(CodeGenerationContext * ctxt,
			     const std::string& aggFn,
			     const IQLToLLVMValue * inc,
			     const FieldType * inputTy,
			     const IQLToLLVMLValue * fieldLVal,
			     const FieldType * ft,
			     llvm::BasicBlock * mergeBlock)=0;
  
public:
  ~NullInitializedAggregate() {}
  void update(CodeGenerationContext * ctxt,
	      const std::string& old,
	      const IQLToLLVMValue * inc,
	      const FieldType * ft) ;
  const IQLToLLVMValue * initialize(CodeGenerationContext * ctxt,
				    const FieldType * ft);  
};

void NullInitializedAggregate::update(CodeGenerationContext * ctxt,
			  const std::string& aggFn,
			  const IQLToLLVMValue * inc,
			  const FieldType * ft)
{
  // Code generate:
  //    IF inc IS NOT NULL THEN (only if input is nullable)
  //      IF old IS NOT NULL THEN 
  //        SET old = old + inc
  //      ELSE
  //        SET old = inc

  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // Merge block 
  llvm::BasicBlock * mergeBlock = 
    llvm::BasicBlock::Create(*c, "cont", TheFunction);
  // LValue around the field
  std::map<std::string, std::pair<std::string, const RecordType *> >& recordTypes(*unwrap(ctxt->IQLRecordArguments));
  const RecordType * outputRecord = recordTypes.find("input1")->second.second;
  BOOST_ASSERT(outputRecord != NULL);
  IQLToLLVMField fieldLVal(ctxt, outputRecord, aggFn, "__BasePointer1__");
  // Type of input
  const FieldType * inputTy = NULL;
  // If a nullable value check for nullability
  llvm::Value * nv = inc->getNull();
  if (nv) {
    // We must skip nullable inputs
    llvm::BasicBlock * updateBlock = 
      llvm::BasicBlock::Create(*c, "aggCkNullInc", TheFunction);
    // Check NULL bit, branch and set block
    b->CreateCondBr(b->CreateNot(nv), updateBlock, mergeBlock);
    // Emit update and branch to merge.
    b->SetInsertPoint(updateBlock);    
    inputTy = ft;
  } else {
    inputTy = ft->clone(false);
  }
  
  llvm::BasicBlock * notNullBlock = 
    llvm::BasicBlock::Create(*c, "aggNotNull", TheFunction);
  llvm::BasicBlock * nullBlock = 
    llvm::BasicBlock::Create(*c, "aggNull", TheFunction);
  const IQLToLLVMValue * old = ctxt->buildVariableRef(aggFn.c_str(), NULL, ft);
  BOOST_ASSERT(old->getNull());
  b->CreateCondBr(b->CreateNot(old->getNull()), notNullBlock, nullBlock);
  // Increment value not null case
  b->SetInsertPoint(notNullBlock);    
  updateNotNull(ctxt, aggFn, inc, inputTy, &fieldLVal, ft, mergeBlock);
  b->CreateBr(mergeBlock);
  // set value null case
  b->SetInsertPoint(nullBlock);    
  updateNull(ctxt, aggFn, inc, inputTy, &fieldLVal, ft);
  b->CreateBr(mergeBlock);

  b->SetInsertPoint(mergeBlock);
}

const IQLToLLVMValue * NullInitializedAggregate::initialize(CodeGenerationContext * ctxt,
					   const FieldType * ft)
{
  return ctxt->buildNull();
}

class SumAggregate : public NullInitializedAggregate
{
protected:
  // Pattern for aggregates that initialize to NULL
  // and skip input NULL values.
  void updateNull(CodeGenerationContext * ctxt,
		  const std::string& aggFn,
		  const IQLToLLVMValue * inc,
		  const FieldType * inputTy,
		  const IQLToLLVMLValue * fieldLVal,
		  const FieldType * ft);
  void updateNotNull(CodeGenerationContext * ctxt,
		     const std::string& aggFn,
		     const IQLToLLVMValue * inc,
		     const FieldType * inputTy,
		     const IQLToLLVMLValue * fieldLVal,
		     const FieldType * ft,
		     llvm::BasicBlock * mergeBlock);
public:
  ~SumAggregate() {}
};

void SumAggregate::updateNull(CodeGenerationContext * ctxt,
			      const std::string& aggFn,
			      const IQLToLLVMValue * inc,
			      const FieldType * inputTy,
			      const IQLToLLVMLValue * fieldLVal,
			      const FieldType * ft)
{
  ctxt->buildSetNullableValue(fieldLVal, inc, ft, false);
}

void SumAggregate::updateNotNull(CodeGenerationContext * ctxt,
				 const std::string& aggFn,
				 const IQLToLLVMValue * inc,
				 const FieldType * inputTy,
				 const IQLToLLVMLValue * fieldLVal,
				 const FieldType * ft,
				 llvm::BasicBlock * mergeBlock)
{
  const IQLToLLVMValue * sum = ctxt->buildAdd(inc,
					      inputTy,
					      ctxt->buildVariableRef(aggFn.c_str(), NULL, ft),
					      ft,
					      ft);
  ctxt->buildSetNullableValue(fieldLVal, sum, ft, false);
}


class MaxMinAggregate : public NullInitializedAggregate
{
private:
  bool mIsMax;
protected:
  // Pattern for aggregates that initialize to NULL
  // and skip input NULL values.
  void updateNull(CodeGenerationContext * ctxt,
		  const std::string& aggFn,
		  const IQLToLLVMValue * inc,
		  const FieldType * inputTy,
		  const IQLToLLVMLValue * fieldLVal,
		  const FieldType * ft);
  void updateNotNull(CodeGenerationContext * ctxt,
		     const std::string& aggFn,
		     const IQLToLLVMValue * inc,
		     const FieldType * inputTy,
		     const IQLToLLVMLValue * fieldLVal,
		     const FieldType * ft,
		     llvm::BasicBlock * mergeBlock);
public:
  MaxMinAggregate(bool isMax);
};

MaxMinAggregate::MaxMinAggregate(bool isMax)
  :
  mIsMax(isMax)
{
}

void MaxMinAggregate::updateNull(CodeGenerationContext * ctxt,
			      const std::string& aggFn,
			      const IQLToLLVMValue * inc,
			      const FieldType * inputTy,
			      const IQLToLLVMLValue * fieldLVal,
			      const FieldType * ft)
{
  ctxt->buildSetNullableValue(fieldLVal, inc, ft, false);
}

void MaxMinAggregate::updateNotNull(CodeGenerationContext * ctxt,
				    const std::string& aggFn,
				    const IQLToLLVMValue * inc,
				    const FieldType * inputTy,
				    const IQLToLLVMLValue * fieldLVal,
				    const FieldType * ft,
				    llvm::BasicBlock * mergeBlock)
{
  // Unwrap to C++
  llvm::LLVMContext * c = llvm::unwrap(ctxt->LLVMContext);
  llvm::IRBuilder<> * b = llvm::unwrap(ctxt->LLVMBuilder);
  // The function we are working on.
  llvm::Function *TheFunction = b->GetInsertBlock()->getParent();
  // A block in which to do the update 
  llvm::BasicBlock * updateBlock = 
    llvm::BasicBlock::Create(*c, "then", TheFunction);

  const IQLToLLVMValue * old = ctxt->buildVariableRef(aggFn.c_str(), NULL, ft);
  const IQLToLLVMValue * condVal = ctxt->buildCompare(old, ft, 
						      inc, inputTy,
						      // Currently returning int32_t for bool
						      Int32Type::Get(ft->getContext(), true),
						      mIsMax ? IQLToLLVMOpLT : IQLToLLVMOpGT);
  // Cast back to i1 by comparing to zero.
  llvm::Value * boolVal = b->CreateICmpNE(condVal->getValue(),
					  b->getInt32(0),
					  "boolCast");
  // Branch and set block
  b->CreateCondBr(boolVal, updateBlock, mergeBlock);
  // Emit update and branch to merge.
  b->SetInsertPoint(updateBlock);    
  ctxt->buildSetValue(inc, aggFn.c_str(), ft);
}

boost::shared_ptr<AggregateFunction> AggregateFunction::get(const char * fn)
{
  AggregateFunction * agg = NULL;
  if (boost::algorithm::iequals(fn, "max")) {
    agg = new MaxMinAggregate(true);
  } else if (boost::algorithm::iequals(fn, "min")) {
    agg = new MaxMinAggregate(false);
  } else if (boost::algorithm::iequals(fn, "sum")) {
    agg = new SumAggregate();
  } else {
    throw std::runtime_error ((boost::format("Unknown aggregate function: %1%")%
			       fn).str());
  }
  return boost::shared_ptr<AggregateFunction>(agg);
}

