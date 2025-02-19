import { defineStore } from 'pinia'
import type { Vehicle, Brand, Model, Year, ReferenceTable } from '~/types/vehicle'

export const useVehicleStore = defineStore('vehicles', {
  state: () => ({
    vehicles: [] as Vehicle[],
    brands: [] as Brand[],
    models: [] as Model[],
    years: [] as Year[],
    referenceTable: [] as ReferenceTable[],
    searchQuery: '',
    filters: {
      brand: null as number | null,
      model: null as number | null,
      year: null as number | null,
      priceRange: [0, 100000] as [number, number]
    }
  }),

  getters: {
    filteredVehicles: (state) => {
      return state.vehicles.filter(vehicle => {
        // Apply search query
        if (state.searchQuery) {
          const searchLower = state.searchQuery.toLowerCase()
          const brand = state.brands.find(b => b.codigo === vehicle.marca_id)
          const model = state.models.find(m => m.codigo === vehicle.modelo_id)
          const searchString = `${brand?.nome || ''} ${model?.nome || ''} ${vehicle.ano_id} ${vehicle.combustivel}`.toLowerCase()
          
          if (!searchString.includes(searchLower)) return false
        }

        // Apply filters
        if (state.filters.brand && vehicle.marca_id !== state.filters.brand) return false
        if (state.filters.model && vehicle.modelo_id !== state.filters.model) return false
        if (state.filters.year && vehicle.ano_id !== state.filters.year) return false
        if (vehicle.preco < state.filters.priceRange[0] || vehicle.preco > state.filters.priceRange[1]) return false
        
        return true
      })
    },
    availableYears: (state) => {
      const years = new Set<number>()
      state.filteredVehicles.forEach(vehicle => {
        years.add(vehicle.ano_id)
      })
      return Array.from(years).sort((a, b) => b - a)
    },
    priceRange: (state) => {
      const prices = state.vehicles.map(v => v.preco)
      return {
        min: Math.min(...prices),
        max: Math.max(...prices)
      }
    }
  },

  actions: {
    setSearchQuery(query: string) {
      this.searchQuery = query
    },
    
    async fetchVehicles() {
      // Mock data for now - replace with actual API calls
      this.vehicles = [
        {
          marca_id: 1,
          modelo_id: 1,
          ano_id: 2024,
          placa_referencia_id: 1,
          codigo_tec: 'ABC123',
          combustivel: 'Gasoline',
          preco: 50000
        },
        {
          marca_id: 1,
          modelo_id: 1,
          ano_id: 2023,
          placa_referencia_id: 1,
          codigo_tec: 'ABC124',
          combustivel: 'Gasoline',
          preco: 45000
        }
      ]

      // Mock brands data
      this.brands = [
        { codigo: 1, nome: 'Toyota' }
      ]

      // Mock models data
      this.models = [
        { codigo: 1, nome: 'Corolla', marca_id: 1 }
      ]
    }
  }
})