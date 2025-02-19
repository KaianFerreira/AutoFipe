export interface Vehicle {
  marca_id: number;
  modelo_id: number;
  ano_id: number;
  placa_referencia_id: number;
  codigo_tec: string;
  combustivel: string;
  preco: number;
}

export interface Brand {
  codigo: number;
  nome: string;
}

export interface Model {
  codigo: number;
  nome: string;
  marca_id: number;
}

export interface Year {
  codigo: number;
  descricao: string;
  modelo_id: number;
  id: number;
}

export interface ReferenceTable {
  codigo: number;
  mes: string;
}