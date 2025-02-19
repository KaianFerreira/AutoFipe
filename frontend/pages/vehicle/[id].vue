<template>
  <div class="max-w-4xl mx-auto">
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg overflow-hidden">
      <div class="aspect-w-16 aspect-h-9 bg-gray-200 dark:bg-gray-700">
        <!-- Placeholder image -->
        <div class="flex items-center justify-center">
          <svg class="w-24 h-24 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
        </div>
      </div>

      <div class="p-6">
        <div class="flex justify-between items-start">
          <div>
            <h1 class="text-2xl font-bold text-gray-900 dark:text-white">
              {{ getBrandName(vehicle?.marca_id) }} {{ getModelName(vehicle?.modelo_id) }}
            </h1>
            <p class="mt-2 text-gray-600 dark:text-gray-300">Year: {{ vehicle?.ano_id }}</p>
          </div>
          <p class="text-2xl font-bold text-gray-900 dark:text-white">
            {{ formatPrice(vehicle?.preco) }}
          </p>
        </div>

        <div class="mt-6">
          <h2 class="text-xl font-semibold text-gray-900 dark:text-white mb-4">Technical Details</h2>
          <dl class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <dt class="text-sm font-medium text-gray-500 dark:text-gray-400">Technical Code</dt>
              <dd class="mt-1 text-sm text-gray-900 dark:text-white">{{ vehicle?.codigo_tec }}</dd>
            </div>
            <div>
              <dt class="text-sm font-medium text-gray-500 dark:text-gray-400">Fuel Type</dt>
              <dd class="mt-1 text-sm text-gray-900 dark:text-white">{{ vehicle?.combustivel }}</dd>
            </div>
            <!-- Add more technical details as needed -->
          </dl>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { storeToRefs } from 'pinia'
import { useVehicleStore } from '~/stores/vehicles'

const route = useRoute()
const store = useVehicleStore()
const { vehicles, brands, models } = storeToRefs(store)

const vehicle = computed(() => {
  return vehicles.value.find(v => v.codigo_tec === route.params.id)
})

const getBrandName = (brandId?: number) => {
  if (!brandId) return 'Unknown Brand'
  const brand = brands.value.find(b => b.codigo === brandId)
  return brand?.nome || 'Unknown Brand'
}

const getModelName = (modelId?: number) => {
  if (!modelId) return 'Unknown Model'
  const model = models.value.find(m => m.codigo === modelId)
  return model?.nome || 'Unknown Model'
}

const formatPrice = (price?: number) => {
  if (!price) return '$0'
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD'
  }).format(price)
}
</script>