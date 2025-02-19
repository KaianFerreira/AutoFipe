<template>
  <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg overflow-hidden hover:shadow-xl transition-shadow duration-300">
    <div class="aspect-w-16 aspect-h-9 bg-gray-200 dark:bg-gray-700">
      <!-- Placeholder image -->
      <div class="flex items-center justify-center">
        <svg class="w-12 h-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
        </svg>
      </div>
    </div>
    <div class="p-4">
      <h3 class="text-lg font-semibold text-gray-900 dark:text-white">{{ brandName }} {{ modelName }}</h3>
      <div class="mt-2 space-y-2">
        <p class="text-sm text-gray-600 dark:text-gray-300">Year: {{ vehicle.ano_id }}</p>
        <p class="text-sm text-gray-600 dark:text-gray-300">Fuel: {{ vehicle.combustivel }}</p>
        <p class="text-lg font-bold text-gray-900 dark:text-white">
          {{ formatPrice(vehicle.preco) }}
        </p>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { Vehicle } from '~/types/vehicle'

const props = defineProps<{
  vehicle: Vehicle
  brandName: string
  modelName: string
}>()

const formatPrice = (price: number) => {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD'
  }).format(price)
}
</script>