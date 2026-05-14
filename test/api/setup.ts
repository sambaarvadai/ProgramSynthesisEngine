import { bootstrap } from '../../src/bootstrap.js'

let services: Awaited<ReturnType<typeof bootstrap>> | null = null

export async function getTestServices() {
  if (!services) {
    services = await bootstrap('test-user')
  }
  return services
}

export function resetTestServices() {
  services = null
}

beforeAll(async () => {
  await getTestServices()
})

afterAll(async () => {
  if (services) {
    await services.crmPool.end()
    if (services.pmPool) {
      await services.pmPool.end()
    }
  }
})
