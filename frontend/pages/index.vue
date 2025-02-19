<template>
  <div class="lg:grid lg:grid-cols-[300px,1fr] lg:gap-6">
    <VehicleFilters class="lg:sticky lg:top-4" />
    
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-32 lg:mb-0">
      <template v-for="vehicle in filteredVehicles" :key="vehicle.codigo_tec">
        <NuxtLink :to="`/vehicle/${vehicle.codigo_tec}`">
          <VehicleCard
            :vehicle="vehicle"
            :brand-name="getBrandName(vehicle.marca_id)"
            :model-name="getModelName(vehicle.modelo_id)"
          />
        </NuxtLink>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { storeToRefs } from 'pinia'
import { useVehicleStore } from '~/stores/vehicles'

const store = useVehicleStore()
const { brands, models, filteredVehicles } = storeToRefs(store)

onMounted(() => {
  store.fetchVehicles()
})

const getBrandName = (brandId: number) => {
  const brand = brands.value.find(b => b.codigo === brandId)
  return brand?.nome || 'Unknown Brand'
}

const getModelName = (modelId: number) => {
  const model = models.value.find(m => m.codigo === modelId)
  return model?.nome || 'Unknown Model'
}
</script>